import os
import csv
import json
import gzip
import requests
import subprocess
from shapely.geometry import Polygon, mapping
import argparse

class PopulationProcessor:
    def __init__(self, download_year: int = 2020):
        self.download_year = download_year
        self.output_folder = "population_output"
        os.makedirs(self.output_folder, exist_ok=True)

    def download_csv(self):
        url = "https://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv"
        print("[STEP 1] Downloading World Bank Population Data\n----------------------------------------")
        zip_path = os.path.join(self.output_folder, "wb_population_data.zip")
        r = requests.get(url)
        with open(zip_path, "wb") as f:
            f.write(r.content)

        subprocess.run(["unzip", "-o", zip_path, "-d", self.output_folder], check=True)
        print("✅ Download complete.")

    def extract_country_population(self):
        print("[STEP 2] Extracting Population Data\n----------------------------------------")
        csv_file = None
        for f in os.listdir(self.output_folder):
            if f.startswith("API_SP.POP.TOTL") and f.endswith(".csv"):
                csv_file = os.path.join(self.output_folder, f)
                break

        if not csv_file:
            raise FileNotFoundError("Population CSV file not found in extracted content")

        pop_data = {}
        with open(csv_file, newline='', encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                country = row["Country Name"]
                value = row.get(str(self.download_year), "")
                if value.isdigit():
                    pop_data[country] = int(value)

        with open(os.path.join(self.output_folder, f"population_{self.download_year}.json"), "w") as f:
            json.dump(pop_data, f, indent=2)
        print(f"✅ Extracted population data for {len(pop_data)} countries.")
        return pop_data

    def generate_geojson(self, pop_data):
        print("[STEP 3] Generating GeoJSON\n----------------------------------------")
        with open("data/countries.geojson") as f:
            countries_geo = json.load(f)

        features = []
        for feat in countries_geo["features"]:
            name = feat["properties"].get("ADMIN")
            population = pop_data.get(name)
            if population is None:
                continue
            feat["properties"]["population"] = population
            feat["properties"]["year"] = self.download_year
            features.append(feat)

        geojson_out = {
            "type": "FeatureCollection",
            "features": features
        }

        out_geojson = os.path.join(self.output_folder, f"population_{self.download_year}_geo.json")
        with open(out_geojson, "w") as f:
            json.dump(geojson_out, f)
        print(f"✅ Saved GeoJSON with {len(features)} features.")
        return out_geojson

    def convert_to_mbtiles(self, geojson_path):
        print("[STEP 4] Generating MBTiles using Tippecanoe\n----------------------------------------")
        mbtiles_path = os.path.join(self.output_folder, f"population_{self.download_year}.mbtiles")
        subprocess.run([
            "tippecanoe",
            "-o", mbtiles_path,
            "-zg",
            "--drop-densest-as-needed",
            "--coalesce-densest-as-needed",
            "--extend-zooms-if-still-dropping",
            geojson_path
        ], check=True)
        print(f"✅ MBTiles generated at {mbtiles_path}")

    def run_pipeline(self):
        self.download_csv()
        pop_data = self.extract_country_population()
        geojson = self.generate_geojson(pop_data)
        self.convert_to_mbtiles(geojson)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="World Bank Population MBTiles Generator")
    parser.add_argument("--downloadyear", type=int, default=2020, help="Year of population data to download (default: 2020)")
    args = parser.parse_args()

    processor = PopulationProcessor(download_year=args.downloadyear)
    processor.run_pipeline()

