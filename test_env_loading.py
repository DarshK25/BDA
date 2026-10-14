"""Test if .env file is being loaded correctly"""
import os
from dotenv import load_dotenv
from pathlib import Path

print("="*60)
print("TESTING .ENV FILE LOADING")
print("="*60)

# Get current directory
current_dir = Path.cwd()
print(f"\nCurrent directory: {current_dir}")

# Check if .env exists
env_path = current_dir / ".env"
print(f".env file exists: {env_path.exists()}")

if env_path.exists():
    print(f".env file path: {env_path}")
    print(f".env file size: {env_path.stat().st_size} bytes")
    
    # Read first few lines
    print("\nFirst few lines of .env:")
    with open(env_path, 'r') as f:
        for i, line in enumerate(f):
            if i < 5:
                # Don't print actual keys, just variable names
                if '=' in line and not line.startswith('#'):
                    var_name = line.split('=')[0]
                    print(f"  {var_name}=...")
                elif line.strip():
                    print(f"  {line.strip()}")

# Load .env
print("\n" + "="*60)
print("LOADING .ENV FILE")
print("="*60)
result = load_dotenv(dotenv_path=env_path, override=True)
print(f"load_dotenv() returned: {result}")

# Check if variables are loaded
print("\n" + "="*60)
print("CHECKING ENVIRONMENT VARIABLES")
print("="*60)

variables = ['TOM_TOM_API_KEY', 'BASEMAPS_API_KEY', 'OSM_TILE_URL']

for var in variables:
    value = os.getenv(var)
    if value:
        # Show first 20 chars only
        display = value[:30] + "..." if len(value) > 30 else value
        print(f"✅ {var}: {display}")
    else:
        print(f"❌ {var}: NOT SET")

print("\n" + "="*60)
print("TEST COMPLETE")
print("="*60)
