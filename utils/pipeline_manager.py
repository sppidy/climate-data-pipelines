#!/usr/bin/env python3
"""
Pipeline Manager for Climate Data Processing
Coordinates the execution of different data type pipelines
"""

import os
import sys
import time
import logging
import subprocess
import argparse
import boto3
from datetime import datetime
from pathlib import Path

# Add scripts directory to Python path
sys.path.insert(0, '/opt/climate-data/scripts')

# Import pipeline configuration
try:
    from pipeline_config import *
except ImportError:
    # Fallback configuration
    AWS_REGION = "ap-south-1"
    CLIMATE_DATA_BUCKET = "climate-data-dev-climate-data-b856a7c3"
    DATA_TYPES = ["precipitation", "humidity", "temperature"]
    DEFAULT_START_YEAR = 2022
    DEFAULT_END_YEAR = 2025
    DEFAULT_START_MONTH = 1
    DEFAULT_END_MONTH = 12
    LOG_LEVEL = "INFO"
    LOG_FILE = "/var/log/climate-pipeline.log"

# Setup logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL if 'LOG_LEVEL' in locals() else 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE if 'LOG_FILE' in locals() else '/var/log/climate-pipeline.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class PipelineManager:
    def __init__(self, start_year=None, end_year=None, start_month=None, end_month=None, 
                 data_types=None, skip_s3_sync=False, verbose=False, dry_run=False):
        self.s3_client = boto3.client('s3', region_name=AWS_REGION)
        self.ec2_client = boto3.client('ec2', region_name=AWS_REGION)
        self.scripts_dir = Path("/opt/climate-data/scripts")
        self.data_dir = Path("/opt/climate-data")
        
        # Processing parameters
        self.start_year = start_year or DEFAULT_START_YEAR
        self.end_year = end_year or DEFAULT_END_YEAR
        self.start_month = start_month or DEFAULT_START_MONTH
        self.end_month = end_month or DEFAULT_END_MONTH
        self.data_types = data_types or DATA_TYPES
        self.skip_s3_sync = skip_s3_sync
        self.verbose = verbose
        self.dry_run = dry_run
        
        logger.info(f"Pipeline Manager initialized with date range: {self.start_year}-{self.start_month:02d} to {self.end_year}-{self.end_month:02d}")
        logger.info(f"Data types to process: {self.data_types}")
        
    def check_s3_access(self):
        """Check if we can access the S3 bucket"""
        if self.dry_run:
            logger.info("[DRY RUN] Would check S3 access")
            return True
            
        try:
            self.s3_client.head_bucket(Bucket=CLIMATE_DATA_BUCKET)
            logger.info(f"Successfully connected to S3 bucket: {CLIMATE_DATA_BUCKET}")
            return True
        except Exception as e:
            logger.error(f"Failed to access S3 bucket {CLIMATE_DATA_BUCKET}: {e}")
            return False
    
    def validate_pipeline_script(self, data_type):
        """Validate that pipeline script exists and is executable"""
        script_path = self.scripts_dir / f"{data_type}_pipeline.py"
        
        if not script_path.exists():
            logger.error(f"Pipeline script not found: {script_path}")
            return False
            
        if not os.access(script_path, os.X_OK):
            logger.warning(f"Pipeline script not executable: {script_path}")
            # Try to make it executable
            try:
                os.chmod(script_path, 0o755)
                logger.info(f"Made script executable: {script_path}")
            except Exception as e:
                logger.error(f"Failed to make script executable: {e}")
                return False
                
        return True
    
    def run_pipeline(self, data_type):
        """Run pipeline for a specific data type"""
        logger.info(f"Starting pipeline for {data_type}")
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would run {data_type} pipeline")
            return True
        
        try:
            # Validate script exists
            if not self.validate_pipeline_script(data_type):
                return False
            
            # Determine script path
            script_path = self.scripts_dir / f"{data_type}_pipeline.py"
            
            # Build command with all available CLI options
            cmd = [
                sys.executable,
                str(script_path),
                "--start-year", str(self.start_year),
                "--end-year", str(self.end_year),
                "--start-month", str(self.start_month),
                "--end-month", str(self.end_month),
                "--output-dir", f"{data_type}_data_output",
                "--mbtiles-dir", f"{data_type}_mbtiles_output"
            ]
            
            # Add verbose flag if requested
            if self.verbose:
                cmd.append("--verbose")
            
            logger.info(f"Running command: {' '.join(cmd)}")
            
            # Run the pipeline script
            result = subprocess.run(
                cmd,
                cwd=str(self.data_dir),
                capture_output=True,
                text=True,
                timeout=7200  # 2 hour timeout (increased for larger datasets)
            )
            
            if result.returncode == 0:
                logger.info(f"Pipeline completed successfully for {data_type}")
                if self.verbose and result.stdout:
                    logger.info(f"Pipeline output: {result.stdout}")
                return True
            else:
                logger.error(f"Pipeline failed for {data_type}")
                logger.error(f"Return code: {result.returncode}")
                if result.stderr:
                    logger.error(f"Error output: {result.stderr}")
                if result.stdout:
                    logger.info(f"Standard output: {result.stdout}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"Pipeline timed out for {data_type} (2 hours)")
            return False
        except Exception as e:
            logger.error(f"Error running pipeline for {data_type}: {e}")
            return False
    
    def sync_to_s3(self, data_type):
        """Sync processed data to S3 (PBF tiles are already uploaded by pipeline)"""
        if self.skip_s3_sync:
            logger.info(f"Skipping S3 sync for {data_type} (--skip-s3-sync)")
            return True
            
        if self.dry_run:
            logger.info(f"[DRY RUN] Would sync {data_type} data to S3")
            return True
            
        logger.info(f"Syncing {data_type} data to S3")
        
        try:
            # Check for different possible output directories
            possible_paths = [
                self.data_dir / f"{data_type}_data_output",
                self.data_dir / "processed-data" / data_type,
                self.data_dir / data_type / "output"
            ]
            
            local_path = None
            for path in possible_paths:
                if path.exists():
                    local_path = path
                    break
            
            if not local_path:
                logger.warning(f"No output directory found for {data_type}")
                logger.info(f"Checked paths: {[str(p) for p in possible_paths]}")
                # Note: PBF tiles are already uploaded by the pipeline, so this is not a failure
                logger.info(f"PBF tiles for {data_type} are automatically uploaded during pipeline execution")
                return True
            
            s3_path = f"s3://{CLIMATE_DATA_BUCKET}/processed-data/{data_type}"
            
            # Sync using AWS CLI
            cmd = ["aws", "s3", "sync", str(local_path), s3_path, "--region", AWS_REGION]
            
            if self.verbose:
                logger.info(f"Running S3 sync command: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"Successfully synced {data_type} data to S3")
                if self.verbose and result.stdout:
                    logger.info(f"Sync output: {result.stdout}")
                return True
            else:
                logger.error(f"Failed to sync {data_type} data")
                if result.stderr:
                    logger.error(f"Sync error: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error syncing {data_type} data: {e}")
            return False
    
    def run_all_pipelines(self):
        """Run all data type pipelines"""
        logger.info("Starting all climate data pipelines")
        
        # Check S3 access first (unless skipping sync)
        if not self.skip_s3_sync and not self.check_s3_access():
            logger.error("Cannot access S3 bucket, exiting")
            return False
        
        success_count = 0
        failed_types = []
        
        for data_type in self.data_types:
            logger.info(f"Processing {data_type} data...")
            
            # Run pipeline
            if self.run_pipeline(data_type):
                # Sync to S3
                if self.sync_to_s3(data_type):
                    success_count += 1
                    logger.info(f"Successfully completed {data_type} pipeline")
                else:
                    logger.error(f"Failed to sync {data_type} data to S3")
                    failed_types.append(data_type)
            else:
                logger.error(f"Failed to run {data_type} pipeline")
                failed_types.append(data_type)
        
        logger.info(f"Pipeline execution completed. {success_count}/{len(self.data_types)} successful")
        
        if failed_types:
            logger.warning(f"Failed data types: {failed_types}")
        
        return success_count == len(self.data_types)

def main():
    """Main function with CLI argument parsing"""
    parser = argparse.ArgumentParser(
        description="Climate Data Pipeline Manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all pipelines with default settings
  python pipeline_manager.py

  # Run specific data types
  python pipeline_manager.py --data-types humidity temperature

  # Custom date range
  python pipeline_manager.py --start-year 2023 --end-year 2024 --start-month 6 --end-month 9

  # Verbose output with dry run
  python pipeline_manager.py --verbose --dry-run

  # Skip S3 sync (for testing)
  python pipeline_manager.py --skip-s3-sync
        """
    )
    
    # Date range options
    parser.add_argument('--start-year', type=int, default=DEFAULT_START_YEAR,
                       help=f'Start year for data processing (default: {DEFAULT_START_YEAR})')
    parser.add_argument('--start-month', type=int, default=DEFAULT_START_MONTH,
                       help=f'Start month for data processing (default: {DEFAULT_START_MONTH})')
    parser.add_argument('--end-year', type=int, default=DEFAULT_END_YEAR,
                       help=f'End year for data processing (default: {DEFAULT_END_YEAR})')
    parser.add_argument('--end-month', type=int, default=DEFAULT_END_MONTH,
                       help=f'End month for data processing (default: {DEFAULT_END_MONTH})')
    
    # Data type options
    parser.add_argument('--data-types', nargs='+', default=DATA_TYPES,
                       help=f'Data types to process (default: {DATA_TYPES})')
    
    # Control options
    parser.add_argument('--skip-s3-sync', action='store_true',
                       help='Skip syncing processed data to S3')
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
    
    # Validate data types
    valid_types = ["precipitation", "humidity", "temperature"]
    invalid_types = [dt for dt in args.data_types if dt not in valid_types]
    if invalid_types:
        print(f"❌ Error: Invalid data types: {invalid_types}")
        print(f"Valid types: {valid_types}")
        sys.exit(1)
    
    logger.info("Starting Climate Data Pipeline Manager")
    
    manager = PipelineManager(
        start_year=args.start_year,
        end_year=args.end_year,
        start_month=args.start_month,
        end_month=args.end_month,
        data_types=args.data_types,
        skip_s3_sync=args.skip_s3_sync,
        verbose=args.verbose,
        dry_run=args.dry_run
    )
    
    try:
        success = manager.run_all_pipelines()
        
        if success:
            logger.info("All pipelines completed successfully")
            return 0
        else:
            logger.error("Some pipelines failed")
            return 1
            
    except KeyboardInterrupt:
        logger.info("Pipeline manager interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error in pipeline manager: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
