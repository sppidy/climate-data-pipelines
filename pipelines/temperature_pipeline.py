#!/usr/bin/env python3
"""
Temperature (T2M) Data Processing Pipeline CLI

Features:
- Downloads NASA POWER T2M data
- Filters for land points only
- Processes CSV to monthly GeoJSON
- Generates MBTiles via tippecanoe
- Produces TileServer-GL config (no HTML viewer)

CLI Flags:
--start-year, --start-month, --end-year, --end-month
--output-dir, --mbtiles-dir
--zoom-min, --zoom-max
--skip-download, --skip-geojson, --skip-mbtiles

Author: Adapted for IITM Internship Project
Date: July 2025
"""

import xarray as xr
import pandas as pd
import os
import time
import traceback
import subprocess
import json
import argparse
from pathlib import Path
from tqdm import tqdm
from global_land_mask import globe

class TemperatureProcessor:
    def __init__(self, args):
        self.ZARR_URL = "s3://nasa-power/merra2/temporal/power_merra2_monthly_temporal_utc.zarr"
        self.VARIABLE = "T2M"  # used for downloading only
        self.PREFIX = "temperature"
        self.start_year = args.start_year
        self.start_month = args.start_month
        self.end_year = args.end_year
        self.end_month = args.end_month
        self.OUTPUT_DIR = args.output_dir
        self.MBTILES_DIR = args.mbtiles_dir
        self.zoom_min = args.zoom_min
        self.zoom_max = args.zoom_max

        os.makedirs(self.OUTPUT_DIR, exist_ok=True)
        os.makedirs(self.MBTILES_DIR, exist_ok=True)

    def check_tippecanoe(self):
        try:
            res = subprocess.run(['tippecanoe', '--version'], capture_output=True, text=True, timeout=10)
            return res.returncode == 0
        except Exception:
            return False

    def time_filter(self, times):
        start = (times.year > self.start_year) | ((times.year == self.start_year) & (times.month >= self.start_month))
        end = (times.year < self.end_year) | ((times.year == self.end_year) & (times.month <= self.end_month))
        return start & end

    def download_data(self):
        print("[STEP 1] Downloading T2M data...")
        try:
            ds = xr.open_dataset(
                self.ZARR_URL,
                engine='zarr',
                backend_kwargs={'consolidated': True, 'storage_options': {'anon': True}}
            )
            times = pd.to_datetime(ds.time.values)
            filt = times[self.time_filter(times)]
            da = ds[self.VARIABLE].sel(time=filt)
            df = da.to_dataframe().reset_index().dropna(subset=[self.VARIABLE])
            csv_path = os.path.join(self.OUTPUT_DIR, f"{self.PREFIX}_monthly_{self.start_year}_{self.end_year}.csv")
            df.to_csv(csv_path, index=False)
            print(f"Saved CSV: {csv_path}")
            return df
        except Exception as e:
            print(f"Download error: {e}")
            traceback.print_exc()
            return None

    def split_monthly(self, df):
        print("[STEP 2] Splitting monthly CSVs...")
        df['year'] = pd.to_datetime(df['time']).dt.year
        df['month'] = pd.to_datetime(df['time']).dt.month
        files = []
        for (year, month), grp in tqdm(df.groupby(['year', 'month']), desc='Monthly files'):
            filename = f"{self.PREFIX}_{month:02d}_{year}.csv"
            path = os.path.join(self.OUTPUT_DIR, filename)
            grp[['time', 'lat', 'lon', self.VARIABLE]].to_csv(path, index=False)
            files.append(path)
        return files

    def csv_to_geojson(self, csv_file, geojson_file):
        df = pd.read_csv(csv_file)
        mask = globe.is_land(df['lat'].values, df['lon'].values)
        df_land = df[mask].dropna(subset=[self.VARIABLE])
        if df_land.empty:
            return False
        lats = sorted(df_land['lat'].unique())
        lons = sorted(df_land['lon'].unique())
        lat_res = abs(lats[1] - lats[0]) if len(lats) > 1 else 0.5
        lon_res = abs(lons[1] - lons[0]) if len(lons) > 1 else 0.625
        features = []
        for _, r in df_land.iterrows():
            lat, lon, val = r['lat'], r['lon'], r[self.VARIABLE]
            half_lat, half_lon = lat_res / 2, lon_res / 2
            coords = [[
                [lon - half_lon, lat - half_lat], [lon + half_lon, lat - half_lat],
                [lon + half_lon, lat + half_lat], [lon - half_lon, lat + half_lat], [lon - half_lon, lat - half_lat]
            ]]
            features.append({
                'type': 'Feature',
                'geometry': {'type': 'Polygon', 'coordinates': coords},
                'properties': {'temperature': float(val), 'time': r['time']}
            })
        with open(geojson_file, 'w') as f:
            json.dump({'type': 'FeatureCollection', 'features': features}, f, separators=(',', ':'))
        return True

    def create_geojsons(self, files):
        print("[STEP 3] Generating GeoJSONs...")
        outs = []
        for f in tqdm(files, desc='GeoJSONs'):
            stem = Path(f).stem  # e.g., 'temperature_06_2022'
            geojson_name = f"{stem}_land.geojson"
            out = os.path.join(self.OUTPUT_DIR, geojson_name)
            if self.csv_to_geojson(f, out):
                outs.append(out)
        return outs

    def geojson_to_mbtiles(self, gj, mb):
        if not self.check_tippecanoe():
            return False
        if os.path.exists(mb):
            os.remove(mb)
        cmd = [
            'tippecanoe', '-o', mb,
            '-Z', str(self.zoom_min), '-z', str(self.zoom_max),
            '--no-feature-limit', '--no-tile-size-limit',
            '--drop-densest-as-needed', '--extend-zooms-if-still-dropping',
            '--force', '--quiet', gj
        ]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
        return r.returncode == 0 and os.path.getsize(mb) > 0

    def create_mbtiles_batch(self, geojsons):
        print("[STEP 4] Generating MBTiles...")
        outs = []
        for gj in tqdm(geojsons, desc='MBTiles'):
            stem = Path(gj).stem  # e.g., 'temperature_06_2022_land'
            mb_name = f"{stem}.mbtiles"
            mb = os.path.join(self.MBTILES_DIR, mb_name)
            if self.geojson_to_mbtiles(gj, mb):
                outs.append(mb)
        return outs

    def create_tileserver_config(self, mbtiles):
        print("[STEP 5] Tileserver config...")
        cfg = {'options': {'paths': {'root': '', 'mbtiles': f"./{self.MBTILES_DIR}"}}, 'data': {}}
        for m in mbtiles:
            name = Path(m).stem  # e.g., 'temperature_06_2022_land'
            cfg['data'][name] = {'mbtiles': Path(m).name}
        with open('temperature-tileserver-config.json', 'w') as f:
            json.dump(cfg, f, indent=2)
        return 'temperature-tileserver-config.json'

    def run(self, args):
        start = time.time()
        df = None
        if not args.skip_download:
            df = self.download_data()
            if df is None:
                return
        else:
            path = os.path.join(self.OUTPUT_DIR, f"{self.PREFIX}_monthly_{self.start_year}_{self.end_year}.csv")
            if not os.path.exists(path):
                return
            df = pd.read_csv(path)
        monthly = self.split_monthly(df)
        geojsons = [] if args.skip_geojson else self.create_geojsons(monthly)
        mbtiles = [] if args.skip_mbtiles else self.create_mbtiles_batch(geojsons)
        cfg = self.create_tileserver_config(mbtiles)
        print(f"Pipeline complete in {time.time() - start:.1f}s: {len(monthly)} CSVs, {len(geojsons)} GeoJSONs, {len(mbtiles)} MBTiles")
        print("Next: tileserver-gl --config", cfg)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="NASA POWER T2M CLI")
    parser.add_argument('--start-year', type=int, default=2022)
    parser.add_argument('--start-month', type=int, default=1)
    parser.add_argument('--end-year', type=int, default=2025)
    parser.add_argument('--end-month', type=int, default=5)
    parser.add_argument('--output-dir', default='temperature_data_output')
    parser.add_argument('--mbtiles-dir', default='temperature_mbtiles_output')
    parser.add_argument('--zoom-min', type=int, default=0)
    parser.add_argument('--zoom-max', type=int, default=10)
    parser.add_argument('--skip-download', action='store_true')
    parser.add_argument('--skip-geojson', action='store_true')
    parser.add_argument('--skip-mbtiles', action='store_true')
    args = parser.parse_args()

    processor = TemperatureProcessor(args)
    processor.run(args)
