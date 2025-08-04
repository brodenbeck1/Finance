#!/usr/bin/env python3
"""
Batch ZST to CSV Converter

This utility script processes multiple ZST files in a directory.
"""

import os
import sys
import argparse
from pathlib import Path
import logging
from zst_to_csv_conversion import convert_zst_to_csv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def find_zst_files(directory: str, recursive: bool = False) -> list:
    """
    Find all ZST files in a directory.
    
    Args:
        directory (str): Directory to search
        recursive (bool): Whether to search recursively
        
    Returns:
        list: List of ZST file paths
    """
    directory = Path(directory)
    
    if not directory.exists():
        raise FileNotFoundError(f"Directory not found: {directory}")
    
    if not directory.is_dir():
        raise ValueError(f"Path is not a directory: {directory}")
    
    pattern = "**/*.zst" if recursive else "*.zst"
    zst_files = list(directory.glob(pattern))
    
    logger.info(f"Found {len(zst_files)} ZST files in {directory}")
    return [str(f) for f in zst_files]


def batch_convert_zst_files(input_dir: str, output_dir: str = None, 
                           recursive: bool = False, keep_intermediate: bool = False) -> dict:
    """
    Convert multiple ZST files to CSV format.
    
    Args:
        input_dir (str): Input directory containing ZST files
        output_dir (str, optional): Output directory for CSV files
        recursive (bool): Whether to search recursively for ZST files
        keep_intermediate (bool): Whether to keep intermediate files
        
    Returns:
        dict: Results summary with success/failure counts
    """
    results = {"success": 0, "failed": 0, "files": []}
    
    # Find all ZST files
    zst_files = find_zst_files(input_dir, recursive)
    
    if not zst_files:
        logger.warning("No ZST files found to convert")
        return results
    
    # Create output directory if specified
    if output_dir:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory: {output_dir}")
    
    # Process each file
    for i, zst_file in enumerate(zst_files, 1):
        file_path = Path(zst_file)
        logger.info(f"Processing file {i}/{len(zst_files)}: {file_path.name}")
        
        try:
            # Determine output path
            if output_dir:
                output_file = Path(output_dir) / file_path.with_suffix('').name
                if not str(output_file).endswith('.csv'):
                    output_file = output_file.with_suffix('.csv')
                output_file = str(output_file)
            else:
                output_file = None
            
            # Convert the file
            result_path = convert_zst_to_csv(zst_file, output_file, keep_intermediate)
            
            results["success"] += 1
            results["files"].append({
                "input": zst_file,
                "output": result_path,
                "status": "success"
            })
            
            logger.info(f"✅ Successfully converted: {file_path.name}")
            
        except Exception as e:
            results["failed"] += 1
            results["files"].append({
                "input": zst_file,
                "output": None,
                "status": "failed",
                "error": str(e)
            })
            
            logger.error(f"❌ Failed to convert {file_path.name}: {e}")
    
    return results


def main():
    """Main entry point for batch conversion."""
    parser = argparse.ArgumentParser(
        description="Batch convert ZST files to CSV format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python batch_zst_converter.py /path/to/zst/files
  python batch_zst_converter.py /path/to/zst/files --output-dir /path/to/csv/files
  python batch_zst_converter.py /path/to/zst/files --recursive --keep-intermediate
        """
    )
    
    parser.add_argument('input_dir', help='Input directory containing ZST files')
    parser.add_argument('--output-dir', '-o', help='Output directory for CSV files')
    parser.add_argument('--recursive', '-r', action='store_true',
                       help='Search for ZST files recursively')
    parser.add_argument('--keep-intermediate', action='store_true',
                       help='Keep intermediate decompressed files')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        results = batch_convert_zst_files(
            args.input_dir,
            args.output_dir,
            args.recursive,
            args.keep_intermediate
        )
        
        # Print summary
        total_files = results["success"] + results["failed"]
        print(f"\n📊 Batch Conversion Summary:")
        print(f"Total files processed: {total_files}")
        print(f"✅ Successfully converted: {results['success']}")
        print(f"❌ Failed conversions: {results['failed']}")
        
        if results["failed"] > 0:
            print(f"\n❌ Failed files:")
            for file_info in results["files"]:
                if file_info["status"] == "failed":
                    print(f"  - {Path(file_info['input']).name}: {file_info['error']}")
        
        if results["success"] > 0:
            print(f"\n✅ Successfully converted files:")
            for file_info in results["files"]:
                if file_info["status"] == "success":
                    input_name = Path(file_info['input']).name
                    output_name = Path(file_info['output']).name
                    print(f"  - {input_name} → {output_name}")
        
    except Exception as e:
        logger.error(f"Batch conversion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
