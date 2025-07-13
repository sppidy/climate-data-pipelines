#!/usr/bin/env python3
"""
Humidity (RH2M) Data Processing Pipeline
Similar to precipitation workflow but for NASA POWER humidity data

This script:
1. Downloads NASA POWER RH2M (Relative Humidity at 2 Meters) data
2. Filters for land points only (excludes ocean areas)
3. Processes CSV data into monthly GeoJSON files with polygon grid cells
4. Generates MBTiles for web visualization using tippecanoe
5. Creates a complete tile server workflow

Workflow: CSV → Land Filtering → Monthly GeoJSON (Polygons) → Monthly MBTiles

Author: Generated for IITM Internship Project
Date: June 2025
"""

import xarray as xr
import pandas as pd
import numpy as np
import os
import time
import traceback
import subprocess
import argparse
import sys
from pathlib import Path
from tqdm import tqdm
import json
from shapely.geometry import Polygon
import geopandas as gpd
from global_land_mask import globe

# Import pipeline configuration
try:
    from pipeline_config import CLIMATE_DATA_BUCKET
except ImportError:
    # Fallback configuration
    CLIMATE_DATA_BUCKET = "climate-data-dev-climate-data-b856a7c3"

class HumidityProcessor:
    def __init__(self, start_year=2022, start_month=1, end_year=2025, end_month=5, 
                 output_dir="humidity_data_output", mbtiles_dir="humidity_mbtiles_output",
                 skip_download=False, skip_geojson=False, skip_mbtiles=False,
                 verbose=False, dry_run=False):
        # Configuration
        self.ZARR_URL = "s3://nasa-power/merra2/temporal/power_merra2_monthly_temporal_utc.zarr"
        self.VARIABLE = "RH2M"
        self.OUTPUT_DIR = output_dir
        self.MBTILES_DIR = mbtiles_dir
        
        # Processing options
        self.start_year = start_year
        self.start_month = start_month
        self.end_year = end_year
        self.end_month = end_month
        self.skip_download = skip_download
        self.skip_geojson = skip_geojson
        self.skip_mbtiles = skip_mbtiles
        self.verbose = verbose
        self.dry_run = dry_run
        
        # Create directories
        if not self.dry_run:
            for directory in [self.OUTPUT_DIR, self.MBTILES_DIR]:
                os.makedirs(directory, exist_ok=True)
        
        if self.verbose:
            print("🌡️ Humidity (RH2M) Data Processing Pipeline")
            print(f"📅 Processing data from {self.start_year}-{self.start_month:02d} to {self.end_year}-{self.end_month:02d}")
            print(f"📁 Output directory: {self.OUTPUT_DIR}")
            print(f"📦 MBTiles directory: {self.MBTILES_DIR}")
            print("=" * 60)

    def time_filter(self, t):
        """Filter for dates from start_year/start_month to end_year/end_month"""
        start_condition = (t.year > self.start_year) | ((t.year == self.start_year) & (t.month >= self.start_month))
        end_condition = (t.year < self.end_year) | ((t.year == self.end_year) & (t.month <= self.end_month))
        return start_condition & end_condition

    def download_humidity_data(self):
        """Download humidity data from NASA POWER Zarr store"""
        if self.dry_run:
            print("[DRY RUN] Would download humidity data")
            return None
            
        print("\n[STEP 1] Downloading NASA POWER RH2M Data")
        print("-" * 40)
        
        try:
            if self.verbose:
                print("📡 Opening remote Zarr store...")
            ds = xr.open_dataset(
                self.ZARR_URL,
                engine="zarr",
                backend_kwargs={
                    "consolidated": True,
                    "storage_options": {"anon": True},
                },
            )
            if self.verbose:
                print("✅ Remote Zarr store opened successfully")
            
            # Filter time
            if self.verbose:
                print("📅 Filtering time range...")
            all_times = pd.to_datetime(ds.time.values)
            filtered_times = all_times[self.time_filter(all_times)]
            
            if filtered_times.empty:
                raise ValueError("No matching data found for specified date range")
            
            if self.verbose:
                print(f"✅ Found {len(filtered_times)} monthly timestamps")
            
            # Extract data
            if self.verbose:
                print("💾 Extracting humidity data...")
            da_subset = ds[self.VARIABLE].sel(time=filtered_times)
            df = da_subset.to_dataframe().reset_index()
            
            # Clean data
            if self.verbose:
                print("🧹 Cleaning data (removing NaNs)...")
            before = len(df)
            df = df.dropna(subset=[self.VARIABLE])
            after = len(df)
            if self.verbose:
                print(f"✅ Cleaned data: {before} → {after} rows ({before-after} NaNs removed)")
            
            # Save main CSV
            main_csv = os.path.join(self.OUTPUT_DIR, f"{self.VARIABLE}_monthly_{self.start_year}_{self.end_year}.csv")
            df.to_csv(main_csv, index=False)
            if self.verbose:
                print(f"💾 Saved main dataset: {main_csv}")
            
            return df
            
        except Exception as e:
            print(f"❌ Error downloading data: {e}")
            if self.verbose:
                traceback.print_exc()
            return None

    def split_monthly_data(self, df):
        """Split the main dataframe into monthly CSV files"""
        if self.dry_run:
            print("[DRY RUN] Would split data into monthly files")
            return []
            
        print("\n[STEP 2] Splitting into Monthly Files")
        print("-" * 40)
        
        monthly_files = []
        
        # Group by year and month
        df['year'] = pd.to_datetime(df['time']).dt.year
        df['month'] = pd.to_datetime(df['time']).dt.month
        
        for (year, month), group in tqdm(df.groupby(['year', 'month']), desc="Creating monthly files"):
            filename = f"humidity_{month:02d}_{year}.csv"
            filepath = os.path.join(self.OUTPUT_DIR, filename)
            
            # Save monthly data
            monthly_data = group[['time', 'lat', 'lon', self.VARIABLE]].copy()
            monthly_data.to_csv(filepath, index=False)
            monthly_files.append(filepath)
        
        if self.verbose:
            print(f"✅ Created {len(monthly_files)} monthly CSV files")
        return monthly_files

    def csv_to_geojson(self, csv_file, output_geojson):
        """Convert CSV humidity data to GeoJSON with polygon grid cells (land only)"""
        try:
            # Read CSV
            df = pd.read_csv(csv_file)
            
            if df.empty:
                if self.verbose:
                    print(f"⚠️ Empty CSV file: {csv_file}")
                return False
            
            if self.verbose:
                print(f"📊 Initial data points: {len(df)}")
            
            # Filter for land points only
            if self.verbose:
                print("🌍 Filtering for land points only...")
            land_mask = globe.is_land(df['lat'].values, df['lon'].values)
            df_land = df[land_mask].copy()
            
            if df_land.empty:
                if self.verbose:
                    print(f"⚠️ No land points found in {csv_file}")
                return False
            
            if self.verbose:
                print(f"🏞️ Land points: {len(df_land)} ({len(df_land)/len(df)*100:.1f}% of total)")
            
            # Get unique coordinates to determine grid resolution
            unique_lats = sorted(df_land['lat'].unique().tolist())
            unique_lons = sorted(df_land['lon'].unique().tolist())
            
            # Calculate grid cell size (assuming regular grid)
            if len(unique_lats) > 1:
                lat_res = abs(unique_lats[1] - unique_lats[0])
            else:
                lat_res = 0.5  # Default resolution
                
            if len(unique_lons) > 1:
                lon_res = abs(unique_lons[1] - unique_lons[0])
            else:
                lon_res = 0.625  # Default resolution
            
            if self.verbose:
                print(f"📐 Grid resolution: {lat_res}° lat × {lon_res}° lon")
            
            # Create polygon features for land points only
            features = []
            for _, row in df_land.iterrows():
                lat = float(row['lat'])
                lon = float(row['lon'])
                humidity = row[self.VARIABLE]
                
                # Skip NaN values
                if pd.isna(humidity):
                    continue
                
                # Create polygon for grid cell (centered on lat/lon)
                half_lat = lat_res / 2
                half_lon = lon_res / 2
                
                # Define polygon coordinates (rectangle)
                coordinates = [[
                    [lon - half_lon, lat - half_lat],  # SW corner
                    [lon + half_lon, lat - half_lat],  # SE corner
                    [lon + half_lon, lat + half_lat],  # NE corner
                    [lon - half_lon, lat + half_lat],  # NW corner
                    [lon - half_lon, lat - half_lat]   # Close polygon
                ]]
                
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": coordinates
                    },
                    "properties": {
                        "humidity": float(humidity),
                        "lat": lat,
                        "lon": lon,
                        "time": str(row['time'])
                    }
                }
                features.append(feature)
            
            # Create GeoJSON structure
            geojson = {
                "type": "FeatureCollection",
                "features": features
            }
            
            # Save GeoJSON
            with open(output_geojson, 'w') as f:
                json.dump(geojson, f, indent=2)
            
            if self.verbose:
                print(f"✅ Created GeoJSON: {output_geojson} ({len(features)} features)")
            return True
            
        except Exception as e:
            print(f"❌ Error converting {csv_file} to GeoJSON: {e}")
            if self.verbose:
                traceback.print_exc()
            return False

    def geojson_to_mbtiles_tippecanoe(self, geojson_file, output_mbtiles):
        """Convert GeoJSON to MBTiles using tippecanoe"""
        try:
            if self.verbose:
                print(f"🔧 Converting {geojson_file} to MBTiles...")
            
            # Tippecanoe command with optimized settings for climate data
            cmd = [
                "tippecanoe",
                "-o", output_mbtiles,
                "-zg",  # Automatically determine zoom levels
                "--drop-densest-as-needed",
                "--extend-zooms-if-still-dropping",
                "--simplification=10",
                "--buffer=64",
                "--accumulate-attribute=humidity:mean",
                geojson_file
            ]
            
            if self.verbose:
                print(f"Running: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                if self.verbose:
                    print(f"✅ Created MBTiles: {output_mbtiles}")
                return True
            else:
                print(f"❌ Tippecanoe failed: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ Error creating MBTiles: {e}")
            if self.verbose:
                traceback.print_exc()
            return False

    def create_geojsons(self, monthly_files):
        """Create GeoJSON files from monthly CSV files"""
        if self.skip_geojson:
            print("\n[STEP 3] Skipping GeoJSON creation (--skip-geojson)")
            return []
            
        if self.dry_run:
            print("[DRY RUN] Would create GeoJSON files")
            return []
            
        print("\n[STEP 3] Creating GeoJSON Files")
        print("-" * 40)
        
        geojson_files = []
        successful = 0
        
        for csv_file in tqdm(monthly_files, desc="Converting to GeoJSON"):
            # Extract year and month from filename
            filename = os.path.basename(csv_file)
            parts = filename.replace('.csv', '').split('_')
            month = parts[1]
            year = parts[2]
            
            output_geojson = os.path.join(self.OUTPUT_DIR, f"humidity_{month}_{year}_land.geojson")
            
            if self.csv_to_geojson(csv_file, output_geojson):
                geojson_files.append(output_geojson)
                successful += 1
        
        if self.verbose:
            print(f"✅ Created {successful}/{len(monthly_files)} GeoJSON files")
        return geojson_files



    def create_mbtiles(self, geojson_files):
        """Create MBTiles from GeoJSON files"""
        if self.skip_mbtiles:
            print("\n[STEP 4] Skipping MBTiles creation (--skip-mbtiles)")
            return []
            
        if self.dry_run:
            print("[DRY RUN] Would create MBTiles files")
            return []
            
        print("\n[STEP 4] Creating MBTiles")
        print("-" * 40)
        
        mbtiles_files = []
        successful = 0
        
        for geojson_file in tqdm(geojson_files, desc="Creating MBTiles"):
            # Extract year and month from filename
            filename = os.path.basename(geojson_file)
            parts = filename.replace('_land.geojson', '').split('_')
            month = parts[1]
            year = parts[2]
            
            output_mbtiles = os.path.join(self.MBTILES_DIR, f"humidity_{month}_{year}_land.mbtiles")
            
            if self.geojson_to_mbtiles_tippecanoe(geojson_file, output_mbtiles):
                mbtiles_files.append(output_mbtiles)
                successful += 1
        
        if self.verbose:
            print(f"✅ Created {successful}/{len(geojson_files)} MBTiles")
        
        return mbtiles_files

    def run_complete_pipeline(self):
        """Run the complete humidity processing pipeline"""
        start_time = time.time()
        
        print("🚀 Starting Complete Humidity Processing Pipeline")
        print("=" * 60)
        
        # Step 1: Download data
        if not self.skip_download:
            df = self.download_humidity_data()
            if df is None:
                print("❌ Pipeline failed at data download step")
                return False
        else:
            print("\n[STEP 1] Skipping data download (--skip-download)")
            # Try to find existing CSV file
            main_csv = os.path.join(self.OUTPUT_DIR, f"{self.VARIABLE}_monthly_{self.start_year}_{self.end_year}.csv")
            if os.path.exists(main_csv):
                df = pd.read_csv(main_csv)
                print(f"✅ Using existing data: {main_csv}")
            else:
                print("❌ No existing data found and download is skipped")
                return False
        
        # Step 2: Split into monthly files
        monthly_files = self.split_monthly_data(df)
        
        # Step 3: Create GeoJSON files
        geojson_files = self.create_geojsons(monthly_files)
        
        # Step 4: Create MBTiles
        mbtiles_files = self.create_mbtiles(geojson_files)
        
        # Summary
        elapsed = time.time() - start_time
        print("\n" + "=" * 60)
        print("🎉 PIPELINE COMPLETE!")
        print(f"⏱️ Total execution time: {elapsed:.2f} seconds")
        print(f"📊 Processed {len(monthly_files)} monthly datasets")
        print(f"🗺️ Created {len(geojson_files)} GeoJSON files")
        print(f"📦 Created {len(mbtiles_files)} MBTiles files")
        print("\n📋 Next steps:")
        print("1. MBTiles are ready for visualization")
        print("2. Use with tileserver-gl or similar for web applications")
        print("3. Files are available in the output directory")
        print("\n💡 Workflow: CSV → Land Filtering → GeoJSON → MBTiles")
        
        return True

def main():
    """Main function with CLI argument parsing"""
    parser = argparse.ArgumentParser(
        description="Humidity (RH2M) Data Processing Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run complete pipeline with default settings
  python humidity_pipeline.py

  # Process specific date range
  python humidity_pipeline.py --start-year 2023 --end-year 2024

  # Skip certain steps (useful for resuming)
  python humidity_pipeline.py --skip-download --skip-geojson

  # Dry run to see what would be done
  python humidity_pipeline.py --dry-run

  # Verbose output
  python humidity_pipeline.py --verbose

  # Custom output directories
  python humidity_pipeline.py --output-dir my_humidity_data --mbtiles-dir my_mbtiles
        """
    )
    
    # Date range options
    parser.add_argument('--start-year', type=int, default=2022,
                       help='Start year for data processing (default: 2022)')
    parser.add_argument('--start-month', type=int, default=1,
                       help='Start month for data processing (default: 1)')
    parser.add_argument('--end-year', type=int, default=2025,
                       help='End year for data processing (default: 2025)')
    parser.add_argument('--end-month', type=int, default=5,
                       help='End month for data processing (default: 5)')
    
    # Output options
    parser.add_argument('--output-dir', default='humidity_data_output',
                       help='Output directory for CSV and GeoJSON files (default: humidity_data_output)')
    parser.add_argument('--mbtiles-dir', default='humidity_mbtiles_output',
                       help='Output directory for MBTiles files (default: humidity_mbtiles_output)')
    
    # Processing options
    parser.add_argument('--skip-download', action='store_true',
                       help='Skip data download step (use existing CSV files)')
    parser.add_argument('--skip-geojson', action='store_true',
                       help='Skip GeoJSON creation step')
    parser.add_argument('--skip-mbtiles', action='store_true',
                       help='Skip MBTiles creation step')
    
    # Control options
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose output')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without actually doing it')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.start_year > args.end_year:
        print("❌ Error: start_year cannot be greater than end_year")
        sys.exit(1)
    
    if args.start_year == args.end_year and args.start_month > args.end_month:
        print("❌ Error: start_month cannot be greater than end_month when start_year equals end_year")
        sys.exit(1)
    
    if not (1 <= args.start_month <= 12):
        print("❌ Error: start_month must be between 1 and 12")
        sys.exit(1)
    
    if not (1 <= args.end_month <= 12):
        print("❌ Error: end_month must be between 1 and 12")
        sys.exit(1)
    
    # Create processor and run pipeline
    processor = HumidityProcessor(
        start_year=args.start_year,
        start_month=args.start_month,
        end_year=args.end_year,
        end_month=args.end_month,
        output_dir=args.output_dir,
        mbtiles_dir=args.mbtiles_dir,
        skip_download=args.skip_download,
        skip_geojson=args.skip_geojson,
        skip_mbtiles=args.skip_mbtiles,
        verbose=args.verbose,
        dry_run=args.dry_run
    )
    
    success = processor.run_complete_pipeline()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
