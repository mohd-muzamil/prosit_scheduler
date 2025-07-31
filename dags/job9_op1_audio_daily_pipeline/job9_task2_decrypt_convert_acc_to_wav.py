import os
import json
from pathlib import Path
from tqdm import tqdm
import subprocess
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import base64
from airflow.models import Variable

# Constants
BASE_PATH = Path("/opt/airflow/dags/job9_op1_audio_daily_pipeline")
ENCRYPTED_DIR = BASE_PATH / "audio_combined" / "encrypted"
DECRYPTED_DIR = BASE_PATH / "audio_combined" / "decrypted"
WAV_DIR = BASE_PATH / "audio_combined" / "wav"
SECRET_KEY = Variable.get("op1_audio_secret_key")

def decrypt_audio_file(input_path, output_path):
    """Decrypt audio file using AES-256-CBC with the provided secret key"""
    try:
        with open(input_path, 'r') as f:
            encrypted_data = f.read().strip()
        
        encrypted_data = json.loads(encrypted_data)
        enc_dict = json.loads(encrypted_data)
        iv = bytes.fromhex(enc_dict['iv'])
        content = bytes.fromhex(enc_dict['content'])
        
        cipher = AES.new(
            bytes.fromhex(SECRET_KEY),
            AES.MODE_CBC,
            iv
        )
        
        decrypted_padded = cipher.decrypt(content)
        decrypted = unpad(decrypted_padded, AES.block_size)
        
        with open(output_path, 'wb') as f:
            f.write(decrypted)
        
        return True
    except json.JSONDecodeError as e:
        print(f"Invalid JSON format in {input_path}: {str(e)}")
        return False
    except Exception as e:
        print(f"Decryption failed for {input_path}: {str(e)}")
        return False

def convert_to_wav(input_path, output_path):
    """Convert audio file to WAV format using ffmpeg"""
    try:
        cmd = [
            'ffmpeg',
            '-y',
            '-i', str(input_path),
            '-acodec', 'pcm_s16le',
            '-ar', '44100',
            '-ac', '1',
            '-hide_banner',
            str(output_path)
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode != 0:
            print(f"Conversion failed for {input_path}: {result.stderr}")
            return False
            
        return True
    except Exception as e:
        print(f"Error converting {input_path}: {str(e)}")
        return False

def process_files():
    """Process all folders in the encrypted directory"""
    # Create output directories
    DECRYPTED_DIR.mkdir(parents=True, exist_ok=True)
    WAV_DIR.mkdir(parents=True, exist_ok=True)
    
    # Find all folders in encrypted directory
    study_folders = [f for f in ENCRYPTED_DIR.iterdir() if f.is_dir()]
    
    if not study_folders:
        print(f"No folders found in {ENCRYPTED_DIR}")
        return
    
    total_files = 0
    skipped_files = 0
    failed_decrypt = 0
    failed_convert = 0
    
    for study_folder in study_folders:
        # Find all .aac files recursively
        aac_files = list(study_folder.rglob("*.aac"))
        
        if not aac_files:
            print(f"No AAC files found in {study_folder}")
            continue
            
        print(f"Processing {len(aac_files)} files in {study_folder.name}")
        
        for aac_file in tqdm(aac_files, desc=study_folder.name):
            # Get relative path from encrypted dir to maintain structure
            relative_path = aac_file.relative_to(ENCRYPTED_DIR)
            
            # Create corresponding paths in decrypted and wav directories
            decrypted_path = DECRYPTED_DIR / relative_path.with_suffix('')
            wav_path = WAV_DIR / relative_path.with_suffix('.wav')
            
            # Create parent directories if they don't exist
            decrypted_path.parent.mkdir(parents=True, exist_ok=True)
            wav_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Check if WAV file already exists
            if wav_path.exists():
                skipped_files += 1
                continue
                
            total_files += 1
                
            # Step 1: Decrypt
            if not decrypt_audio_file(aac_file, decrypted_path.with_suffix('.aac')):
                failed_decrypt += 1
                continue
                
            # Step 2: Convert to WAV
            if not convert_to_wav(decrypted_path.with_suffix('.aac'), wav_path):
                failed_convert += 1
    
    # Print summary
    processed_files = total_files - skipped_files
    success_count = processed_files - (failed_decrypt + failed_convert)
    success_rate = (success_count / processed_files * 100) if processed_files else 0
    
    print("\nProcessing complete:")
    print(f"Total files found: {total_files + skipped_files}")
    print(f"Skipped (already processed): {skipped_files}")
    print(f"Attempted to process: {processed_files}")
    print(f"Successfully processed: {success_count}")
    print(f"Failed decryption: {failed_decrypt}")
    print(f"Failed conversion: {failed_convert}")
    print(f"Success rate: {success_rate:.1f}%")

if __name__ == "__main__":
    print("Starting audio processing pipeline")
    process_files()
    print("Processing completed")