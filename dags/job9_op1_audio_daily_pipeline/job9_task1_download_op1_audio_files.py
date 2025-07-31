import subprocess
from pathlib import Path
import shutil
import re

REMOTE_HOST = "prositadmin@op1.research.cs.dal.ca"
CONTAINER_NAME = "prosit_universal_api"
REMOTE_UPLOAD_DIR = "/usr/src/prosit_universal_app/uploads"
REMOTE_TMP = "/home/prositadmin/docker_audio_temp"

LOCAL_BASE_DIR = "/opt/airflow/dags/job9_op1_audio_daily_pipeline"
LOCAL_ENCRYPTED_DIR = Path(LOCAL_BASE_DIR) / "audio_combined/encrypted"
LOCAL_TMP = Path(LOCAL_BASE_DIR) / "temp_sync_op1"

SSH_OPTS = [
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "ConnectTimeout=30",
    "-o", "ServerAliveInterval=60",
    "-o", "ServerAliveCountMax=3"
]
SSH = ["ssh"] + SSH_OPTS

def run_cmd(cmd, **kwargs):
    """Run subprocess command with error handling"""
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True, **kwargs)

def get_study_prefix(participant_id):
    """Extract study prefix from participant ID"""
    # Example: prositms00023 -> prositms
    #          prositpsa017 -> prositps
    match = re.match(r'^([a-zA-Z]+)', participant_id)
    return match.group(1).lower() if match else 'other'

def download_audio_files():
    LOCAL_TMP.mkdir(parents=True, exist_ok=True)
    LOCAL_ENCRYPTED_DIR.mkdir(parents=True, exist_ok=True)

    try:
        print("Preparing remote temp directory...")
        run_cmd(SSH + [REMOTE_HOST, f"rm -rf {REMOTE_TMP} && mkdir -p {REMOTE_TMP}"])

        print("Copying .aac files from container to remote temp...")
        copy_cmd = (
            f"ssh {' '.join(SSH_OPTS)} {REMOTE_HOST} "
            f"\"docker exec {CONTAINER_NAME} bash -c 'cd {REMOTE_UPLOAD_DIR} && "
            f"find . -type f -name \\*.aac -exec tar -czf - {{}} +'\" | "
            f"ssh {' '.join(SSH_OPTS)} {REMOTE_HOST} \"tar -xzf - -C {REMOTE_TMP}\""
        )
        result = subprocess.run(copy_cmd, shell=True, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            if "No such file or directory" not in result.stderr:
                raise RuntimeError("Copy from container failed")
        else:
            print("Files copied successfully.")

        print("Syncing to local...")
        run_cmd(["rsync", "-avz", "--ignore-existing", f"{REMOTE_HOST}:{REMOTE_TMP}/", str(LOCAL_TMP)])

        print("Organizing files by participant ID...")
        for filepath in LOCAL_TMP.rglob("*.aac"):
            # Get participant ID from parent directory name
            participant_id = filepath.parent.name
            
            # Skip if participant ID can't be determined
            if not participant_id:
                print(f"Warning: Cannot determine participant ID from {filepath}")
                continue
            
            # Determine study prefix (e.g., prositms, prositps)
            study_prefix = get_study_prefix(participant_id)
            
            # Create study directory if needed
            study_dir = LOCAL_ENCRYPTED_DIR / study_prefix
            study_dir.mkdir(parents=True, exist_ok=True)
            
            # Create new filename with participant ID prefix
            new_filename = f"{participant_id}_{filepath.name}"
            target = study_dir / new_filename

            if not target.exists():
                shutil.copy2(filepath, target)
                print(f"Copied: {target}")
            else:
                print(f"Skipped (exists): {target}")

    finally:
        print("Cleaning up temp...")
        shutil.rmtree(LOCAL_TMP, ignore_errors=True)
        subprocess.run(SSH + [REMOTE_HOST, f"rm -rf {REMOTE_TMP}"], check=False)

if __name__ == "__main__":
    download_audio_files()