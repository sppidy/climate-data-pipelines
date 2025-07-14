#!/usr/bin/env python3
"""
NASA POWER RH2M Humidity Data Processing Pipeline
CLI-compatible | Land-Only | MBTiles | TileServer Config | No HTML Viewer
"""

import xarray as xr
import pandas as pd
import numpy as np
import os
import time
import traceback
import subprocess
import shutil
from pathlib import Path
from tqdm import tqdm
import json
from global_land_mask import globe
import argparse

class HumidityProcessor:
    def __init__(self):
        self.ZARR_URL = "s3://nasa-power/merra2/temporal/power_merra2_monthly_temporal_utc.zarr"
        self.VARIABLE = "RH2M"
        self.OUTPUT_DIR = "humidity_data_output"
        self.MBTILES_DIR = "humidity_mbtiles_output"
        self.start_year = 2022
        self.start_month = 1
        self.end_year = 2025
        self.end_month = 5

    def check_tippecanoe_installation(self):
        """Check if tippecanoe is installed and accessible"""
        try:
            result = subprocess.run(['tippecanoe', '--version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                print(f"✅ Tippecanoe found: {result.stdout.strip()}")
                return True
            else:
                print("❌ Tippecanoe not working properly")
                return False
        except FileNotFoundError:
            print("❌ Tippecanoe not found. Install with: brew install tippecanoe (macOS) or apt-get install tippecanoe (Ubuntu)")
            return False
        except subprocess.TimeoutExpired:
            print("❌ Tippecanoe version check timed out")
            return False

    def time_filter(self, t):
        start = (t.year > self.start_year) | ((t.year == self.start_year) & (t.month >= self.start_month))
        end = (t.year < self.end_year) | ((t.year == self.end_year) & (t.month <= self.end_month))
        return start & end

    def download_humidity_data(self):
        print("\n[STEP 1] Downloading NASA POWER RH2M Data")
        try:
            ds = xr.open_dataset(
                self.ZARR_URL,
                engine="zarr",
                backend_kwargs={"consolidated": True, "storage_options": {"anon": True}},
            )
            all_times = pd.to_datetime(ds.time.values)
            filtered_times = all_times[self.time_filter(all_times)]
            if filtered_times.empty:
                raise ValueError("No matching data found for specified date range")

            da_subset = ds[self.VARIABLE].sel(time=filtered_times)
            df = da_subset.to_dataframe().reset_index()
            df = df.dropna(subset=[self.VARIABLE])

            os.makedirs(self.OUTPUT_DIR, exist_ok=True)
            csv_path = os.path.join(self.OUTPUT_DIR, f"{self.VARIABLE}_monthly_{self.start_year}_{self.end_year}.csv")
            df.to_csv(csv_path, index=False)
            print(f"✅ Saved main dataset: {csv_path}")
            return df
        except Exception as e:
            print(f"❌ Error: {e}")
            traceback.print_exc()
            return None

    def split_monthly_data(self, df):
        df['year'] = pd.to_datetime(df['time']).dt.year
        df['month'] = pd.to_datetime(df['time']).dt.month
        monthly_files = []

        for (year, month), group in df.groupby(['year', 'month']):
            filename = f"humidity_{month:02d}_{year}.csv"
            filepath = os.path.join(self.OUTPUT_DIR, filename)
            group[['time', 'lat', 'lon', self.VARIABLE]].to_csv(filepath, index=False)
            monthly_files.append(filepath)

        return monthly_files

    def validate_geojson(self, geojson_file):
        """Validate GeoJSON file has features"""
        try:
            with open(geojson_file, 'r') as f:
                geojson = json.load(f)
            
            features = geojson.get('features', [])
            if not features:
                print(f"❌ No features in {geojson_file}")
                return False
            
            print(f"✅ {geojson_file} has {len(features)} features")
            return True
        except Exception as e:
            print(f"❌ Invalid GeoJSON {geojson_file}: {e}")
            return False

    def csv_to_geojson(self, csv_file, output_geojson):
        try:
            df = pd.read_csv(csv_file)
            print(f"📊 Processing {csv_file}: {len(df)} rows")
            
            if df.empty:
                print(f"❌ Empty CSV: {csv_file}")
                return False

            # Apply land mask
            land_mask = globe.is_land(df['lat'].values, df['lon'].values)
            df_land = df[land_mask].copy()
            print(f"🌍 Land points: {len(df_land)} out of {len(df)}")
            
            if df_land.empty:
                print(f"❌ No land points in {csv_file}")
                return False

            # Remove any remaining NaN values
            df_land = df_land.dropna(subset=[self.VARIABLE])
            if df_land.empty:
                print(f"❌ No valid humidity data after NaN removal in {csv_file}")
                return False

            unique_lats = sorted(df_land['lat'].unique())
            unique_lons = sorted(df_land['lon'].unique())
            lat_res = abs(unique_lats[1] - unique_lats[0]) if len(unique_lats) > 1 else 0.5
            lon_res = abs(unique_lons[1] - unique_lons[0]) if len(unique_lons) > 1 else 0.625

            features = []
            for _, row in df_land.iterrows():
                lat, lon, humidity = row['lat'], row['lon'], row[self.VARIABLE]
                if pd.isna(humidity): 
                    continue
                    
                half_lat, half_lon = lat_res / 2, lon_res / 2
                coordinates = [[
                    [lon - half_lon, lat - half_lat],
                    [lon + half_lon, lat - half_lat],
                    [lon + half_lon, lat + half_lat],
                    [lon - half_lon, lat + half_lat],
                    [lon - half_lon, lat - half_lat]
                ]]
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "Polygon", "coordinates": coordinates},
                    "properties": {
                        "humidity": float(humidity),  # Ensure numeric
                        "time": str(row['time']),
                        "lat": float(lat),
                        "lon": float(lon)
                    }
                })

            if not features:
                print(f"❌ No valid features generated for {csv_file}")
                return False

            geojson = {"type": "FeatureCollection", "features": features}
            with open(output_geojson, 'w') as f:
                json.dump(geojson, f, separators=(',', ':'))  # Compact JSON
            
            print(f"✅ Created GeoJSON: {output_geojson} with {len(features)} features")
            return True
            
        except Exception as e:
            print(f"❌ GeoJSON error for {csv_file}: {e}")
            traceback.print_exc()
            return False

    def geojson_to_mbtiles_tippecanoe(self, geojson_file, output_mbtiles, zoom_min=0, zoom_max=10):
        try:
            # Check if tippecanoe is available
            if not self.check_tippecanoe_installation():
                return False
                
            # Validate input file
            if not self.validate_geojson(geojson_file):
                return False
            
            # Remove existing mbtiles file if it exists
            if os.path.exists(output_mbtiles):
                os.remove(output_mbtiles)
                print(f"🗑️  Removed existing: {output_mbtiles}")
            
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_mbtiles), exist_ok=True)
            
            # Build tippecanoe command with better options
            cmd = [
                'tippecanoe',
                '-o', output_mbtiles,
                '-z', str(zoom_max),
                '-Z', str(zoom_min),
                '--no-feature-limit',
                '--no-tile-size-limit',
                '--drop-densest-as-needed',
                '--extend-zooms-if-still-dropping',
                '--force',
                '--quiet',  # Reduce output noise
                geojson_file
            ]
            
            print(f"🔧 Running: {' '.join(cmd)}")
            
            # Run tippecanoe with better error handling
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
            
            if result.returncode == 0:
                # Verify output file was created
                if os.path.exists(output_mbtiles) and os.path.getsize(output_mbtiles) > 0:
                    file_size = os.path.getsize(output_mbtiles) / 1024 / 1024  # MB
                    print(f"✅ Created MBTiles: {output_mbtiles} ({file_size:.1f} MB)")
                    return True
                else:
                    print(f"❌ MBTiles file not created or empty: {output_mbtiles}")
                    return False
            else:
                print(f"❌ Tippecanoe failed with return code {result.returncode}")
                if result.stdout:
                    print(f"   STDOUT: {result.stdout}")
                if result.stderr:
                    print(f"   STDERR: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print(f"❌ Tippecanoe timed out after 30 minutes")
            return False
        except Exception as e:
            print(f"❌ Tippecanoe error: {e}")
            traceback.print_exc()
            return False

    def create_geojsons(self, monthly_files):
        print(f"\n[STEP 2] Creating GeoJSON files for {len(monthly_files)} monthly files")
        geojson_files = []
        
        for i, csv_file in enumerate(monthly_files, 1):
            base_name = os.path.basename(csv_file).replace('.csv', '')
            geojson_file = os.path.join(self.OUTPUT_DIR, f"{base_name}_land.geojson")
            
            print(f"📋 [{i}/{len(monthly_files)}] Processing: {csv_file}")
            
            if self.csv_to_geojson(csv_file, geojson_file):
                geojson_files.append(geojson_file)
            else:
                print(f"⚠️  Skipped: {csv_file}")
        
        print(f"✅ Created {len(geojson_files)} GeoJSON files")
        return geojson_files

    def create_mbtiles(self, geojson_files, zoom_min=0, zoom_max=10):
        print(f"\n[STEP 3] Creating MBTiles for {len(geojson_files)} GeoJSON files")
        mbtiles_files = []
        
        for i, geojson_file in enumerate(geojson_files, 1):
            base_name = os.path.basename(geojson_file).replace('_land.geojson', '')
            mbtiles_file = os.path.join(self.MBTILES_DIR, f"{base_name}_land.mbtiles")
            
            print(f"🗺️  [{i}/{len(geojson_files)}] Processing: {geojson_file}")
            
            if self.geojson_to_mbtiles_tippecanoe(geojson_file, mbtiles_file, zoom_min, zoom_max):
                mbtiles_files.append(mbtiles_file)
            else:
                print(f"⚠️  Skipped: {geojson_file}")
        
        print(f"✅ Created {len(mbtiles_files)} MBTiles files")
        return mbtiles_files

    def create_tileserver_config(self, mbtiles_files):
        print(f"\n[STEP 4] Creating TileServer GL config")
        config = {
            "options": {
                "paths": {"root": "", "mbtiles": f"./{self.MBTILES_DIR}"},
                "serveStaticMaps": True,
                "formatQuality": {"jpeg": 90, "webp": 90},
                "maxSize": 8192
            },
            "data": {}
        }
        
        for mbtiles_file in mbtiles_files:
            base_name = os.path.basename(mbtiles_file).replace('_land.mbtiles', '')
            config["data"][f"{base_name}_land"] = {
                "mbtiles": os.path.basename(mbtiles_file)
            }
        
        config_file = "humidity-tileserver-config.json"
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"✅ Created tileserver config: {config_file}")
        print(f"📋 Configured {len(mbtiles_files)} tilesets")

def main():
    parser = argparse.ArgumentParser(description="NASA POWER RH2M Humidity Data CLI (Land-Only Tiles)")
    parser.add_argument('--start-year', type=int, default=2022)
    parser.add_argument('--start-month', type=int, default=1)
    parser.add_argument('--end-year', type=int, default=2025)
    parser.add_argument('--end-month', type=int, default=5)
    parser.add_argument('--output-dir', type=str, default='humidity_data_output')
    parser.add_argument('--mbtiles-dir', type=str, default='humidity_mbtiles_output')
    parser.add_argument('--zoom-min', type=int, default=0)
    parser.add_argument('--zoom-max', type=int, default=10)
    parser.add_argument('--skip-download', action='store_true')
    parser.add_argument('--skip-geojson', action='store_true')
    parser.add_argument('--skip-mbtiles', action='store_true')
    args = parser.parse_args()

    processor = HumidityProcessor()
    processor.start_year = args.start_year
    processor.start_month = args.start_month
    processor.end_year = args.end_year
    processor.end_month = args.end_month
    processor.OUTPUT_DIR = args.output_dir
    processor.MBTILES_DIR = args.mbtiles_dir

    # Create directories
    os.makedirs(processor.OUTPUT_DIR, exist_ok=True)
    os.makedirs(processor.MBTILES_DIR, exist_ok=True)

    # Check tippecanoe installation early
    if not args.skip_mbtiles:
        if not processor.check_tippecanoe_installation():
            print("❌ Cannot proceed without tippecanoe. Install it or use --skip-mbtiles")
            return

    # Download or load data
    df = None
    if not args.skip_download:
        df = processor.download_humidity_data()
        if df is None:
            print("❌ Download failed.")
            return
    else:
        csv_path = os.path.join(processor.OUTPUT_DIR, f"{processor.VARIABLE}_monthly_{processor.start_year}_{processor.end_year}.csv")
        if not os.path.exists(csv_path):
            print(f"❌ CSV file not found: {csv_path}")
            return
        df = pd.read_csv(csv_path)

    # Process data
    monthly_files = processor.split_monthly_data(df)
    print(f"📊 Split into {len(monthly_files)} monthly files")

    geojson_files = [] if args.skip_geojson else processor.create_geojsons(monthly_files)
    mbtiles_files = [] if args.skip_mbtiles else processor.create_mbtiles(geojson_files, args.zoom_min, args.zoom_max)
    
    processor.create_tileserver_config(mbtiles_files)

    print("\n🎉 CLI Pipeline Complete!")
    print(f"📁 Data files: {processor.OUTPUT_DIR}")
    print(f"🗺️  MBTiles: {processor.MBTILES_DIR}")
    print(f"⚙️  Config: humidity-tileserver-config.json")

if __name__ == "__main__":
    main()
