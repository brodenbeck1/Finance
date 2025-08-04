#!/usr/bin/env python3
"""
Quick test script for dbt + PostgreSQL setup
"""

import os
import subprocess
import sys
from pathlib import Path

def run_command(command, cwd=None):
    """Run a shell command and return success status."""
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            cwd=cwd,
            capture_output=True, 
            text=True
        )
        
        if result.returncode == 0:
            print(f"✅ {command}")
            return True
        else:
            print(f"❌ {command}")
            print(f"Error: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ {command} - Exception: {e}")
        return False

def main():
    """Test the dbt setup."""
    print("🧪 Testing dbt + PostgreSQL setup...\n")
    
    # Change to dbt project directory
    dbt_dir = Path("dbt/")
    
    if not dbt_dir.exists():
        print(f"❌ dbt project directory not found: {dbt_dir}")
        return False
    
    # Test dbt configuration
    print("1. Testing dbt configuration...")
    if not run_command("dbt debug", cwd=dbt_dir):
        print("💡 Make sure PostgreSQL is running and credentials are set")
        return False
    
    # Parse dbt models
    print("\n2. Parsing dbt models...")
    if not run_command("dbt parse", cwd=dbt_dir):
        return False
    
    # Compile dbt models (without running)
    print("\n3. Compiling dbt models...")
    if not run_command("dbt compile", cwd=dbt_dir):
        return False
    
    print("\n✅ dbt setup test completed successfully!")
    print("\n🚀 Next steps:")
    print("1. Make sure PostgreSQL is running")
    print("2. Run: python setup_database.py")
    print("3. Run: cd trading_analytics && dbt run")
    print("4. Run: dbt test")
    print("5. Run: dbt docs generate && dbt docs serve")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
