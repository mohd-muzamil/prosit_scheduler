import shutil
from pathlib import Path
from datetime import datetime
import re

WAV_DIR = Path("/opt/airflow/dags/job9_op1_audio_daily_pipeline/audio_combined/wav")
STAGING_DIR = Path("/opt/airflow/dags/job9_op1_audio_daily_pipeline/vidar_sync/op1_audio_files")

def convert_timestamp(ts):
    """Convert millisecond timestamp to YYYY-MM-DD_HH-MM-SS format"""
    return datetime.utcfromtimestamp(int(ts)/1000).strftime("%Y-%m-%d_%H-%M-%S")

def prepare_files():
    """Add human-readable timestamps to filenames in correct order"""
    # Pattern to extract components from original filename
    pattern = r'(.*)_file-(\d+)-([a-zA-Z_]+)_(\d+)\.wav'
    
    # Create clean staging directory
    if STAGING_DIR.exists():
        shutil.rmtree(STAGING_DIR)
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    
    processed = 0
    skipped = 0
    
    for src_path in WAV_DIR.rglob("*.wav"):
        # Extract components from filename
        match = re.match(pattern, src_path.name)
        if not match:
            print(f"Skipping invalid filename: {src_path.name}")
            continue
            
        prefix, uploaded_ts, label, measured_ts = match.groups()
        
        # Create new filename with correct timestamp order
        new_name = (f"{prefix}_file-{uploaded_ts}-{label}_{measured_ts}"
                   f"_measuredat_{convert_timestamp(measured_ts)}"
                   f"_uploadedat_{convert_timestamp(uploaded_ts)}.wav")
        
        # Maintain same directory structure in target
        relative_path = src_path.relative_to(WAV_DIR)
        target_path = STAGING_DIR / relative_path.with_name(new_name)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Skip if file already exists
        if target_path.exists():
            skipped += 1
            continue
            
        # Copy file
        shutil.copy2(src_path, target_path)
        processed += 1
        print(f"Processed: {target_path}")
    
    print(f"\nSummary: {processed} files processed, {skipped} files already existed")

if __name__ == "__main__":
    prepare_files()