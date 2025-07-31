# Job9 Audio Pipeline Deployment Guide

## Overview
Job9 is an Airflow DAG that processes audio files from the op1 server and syncs them to vidar.research.cs.dal.ca. The pipeline consists of four main steps:

1. **Download**: Downloads encrypted .aac files from op1 server (integrated as PythonOperator)
2. **Decrypt/Convert**: Decrypts and converts files to WAV format
3. **Prepare**: Organizes WAV files for sync to vidar
4. **Sync**: Uploads WAV files to nathan@vidar.research.cs.dal.ca:/home/nathan/op1_audio_files/

## Prerequisites

### 1. Directory Structure
Ensure the following directories exist:
```bash
mkdir -p /home/prositadmin/audio_processing/audio_combined/{encrypted,decrypted,wav}
```

### 2. Airflow Variables
Set the required Airflow variable:
```bash
# In Airflow UI or CLI:
airflow variables set AUDIO_SECRET_KEY "your_secret_key_here"
```

**Note**: The variable name has been changed from `op1_audio_secret_key` to `AUDIO_SECRET_KEY` to match the updated implementation.

### 3. SSH Key Authentication
Set up SSH key authentication for both op1 and vidar servers:

```bash
# Generate SSH key if not exists
ssh-keygen -t rsa -b 4096 -C "prositadmin@prosit_scheduler"

# Copy public key to op1 server (for download step)
ssh-copy-id prositadmin@op1.research.cs.dal.ca

# Copy public key to vidar server (for sync step)
ssh-copy-id nathan@vidar.research.cs.dal.ca

# Test connections
ssh prositadmin@op1.research.cs.dal.ca "echo 'OP1 SSH connection successful'"
ssh nathan@vidar.research.cs.dal.ca "echo 'Vidar SSH connection successful'"
```

### 4. Dependencies
Ensure the following Python packages are installed:
- `pycryptodome` (for AES decryption)
- `tqdm` (for progress bars)

```bash
pip install pycryptodome tqdm
```

### 5. System Dependencies
Ensure `ffmpeg` is installed for audio conversion:
```bash
sudo apt-get update
sudo apt-get install ffmpeg
```

## Configuration

### Pipeline Settings
- **Schedule**: Daily (`@daily`)
- **Start Date**: 1 day ago
- **Catchup**: Disabled
- **Tags**: `['audio', 'op1', 'sync']`

### File Paths
- **Main DAG**: `/home/prositadmin/apps/prosit_scheduler/dags/job9_op1_audio_daily_pipeline/job9_dag_audio_daily_pipeline.py`
- **Download Task**: `/home/prositadmin/apps/prosit_scheduler/dags/job9_op1_audio_daily_pipeline/job9_task1_download_op1_audio_files.py`
- **Conversion Task**: `/home/prositadmin/apps/prosit_scheduler/dags/job9_op1_audio_daily_pipeline/job9_task2_decrypt_convert_acc_to_wav.py`
- **Prepare Task**: `/home/prositadmin/apps/prosit_scheduler/dags/job9_op1_audio_daily_pipeline/job9_task3_prepare_vidar_wav_sync.py`
- **Working Directory**: `/opt/airflow/dags/job9_op1_audio_daily_pipeline/`
- **Sync Destination**: `nathan@vidar.research.cs.dal.ca:/home/nathan/op1_audio_files/`

## Deployment Steps

### 1. Verify DAG Syntax
```bash
cd /home/prositadmin/apps/prosit_scheduler/dags/job9_op1_audio_daily_pipeline/
python3 -m py_compile job9_dag_audio_daily_pipeline.py
```

### 2. Test Individual Components
```bash
# Test download task
python3 -m py_compile job9_task1_download_op1_audio_files.py

# Test conversion task
python3 -m py_compile job9_task2_decrypt_convert_acc_to_wav.py

# Test prepare task
python3 -m py_compile job9_task3_prepare_vidar_wav_sync.py

# Test download functionality manually
python3 job9_task1_download_op1_audio_files.py
```

### 3. Enable DAG in Airflow
1. Copy DAG files to Airflow DAGs directory
2. Refresh Airflow UI
3. Enable the `audio_pipeline_op1_to_vidar` DAG
4. Monitor first execution

## Monitoring

### Log Files
- **Airflow Task Logs**: Available in Airflow UI for all tasks (download, decrypt/convert, prepare, sync)
- **Individual Task Logs**: Each PythonOperator logs to Airflow's standard logging system

### Key Metrics to Monitor
- Number of files downloaded from op1 server
- File organization by study prefix (prositms, prositps, etc.)
- Decryption success rate
- Conversion success rate
- File preparation for sync
- Sync completion status to vidar
- Disk space usage in `/opt/airflow/dags/job9_op1_audio_daily_pipeline/`

## Troubleshooting

### Common Issues

#### 1. SSH Permission Denied
**Error**: `Permission denied (publickey,password)`
**Solution**: Set up SSH key authentication (see Prerequisites #3)

#### 2. Missing Directories
**Error**: `No such file or directory`
**Solution**: Create required directory structure (see Prerequisites #1)

#### 3. Decryption Failures
**Error**: `Decryption failed`
**Solutions**:
- Verify `AUDIO_SECRET_KEY` Airflow variable is set correctly
- Check if source files are properly encrypted
- Verify file format matches expected JSON structure

#### 4. Conversion Failures
**Error**: `ffmpeg` conversion errors
**Solutions**:
- Ensure `ffmpeg` is installed
- Check if decrypted files are valid audio format
- Verify sufficient disk space

#### 5. Download Failures
**Error**: SSH connection to op1 server fails
**Solutions**:
- Test SSH connection: `ssh prositadmin@op1.research.cs.dal.ca`
- Verify SSH key is properly configured for op1 server
- Check if Docker container `prosit_universal_api` is running on op1
- Verify network connectivity to op1.research.cs.dal.ca

#### 6. Sync Failures
**Error**: `rsync` connection issues
**Solutions**:
- Test SSH connection: `ssh nathan@vidar.research.cs.dal.ca`
- Verify destination directory `/home/nathan/op1_audio_files/` exists on vidar server
- Check network connectivity to vidar.research.cs.dal.ca

### Manual Testing Commands
```bash
# Test SSH connections
ssh prositadmin@op1.research.cs.dal.ca "echo 'OP1 connection OK'"
ssh nathan@vidar.research.cs.dal.ca "ls -la /home/nathan/op1_audio_files/"

# Test Docker container access on op1
ssh prositadmin@op1.research.cs.dal.ca "docker exec prosit_universal_api ls /usr/src/prosit_universal_app/uploads"

# Test rsync to vidar (dry run)
rsync -avz --dry-run /opt/airflow/dags/job9_op1_audio_daily_pipeline/vidar_sync/op1_audio_files/ nathan@vidar.research.cs.dal.ca:/home/nathan/op1_audio_files/

# Check disk space
df -h /opt/airflow/dags/job9_op1_audio_daily_pipeline/

# View Airflow task logs
airflow tasks log audio_pipeline_op1_to_vidar download_audio_files <execution_date>
```

## Performance Considerations

### Disk Space Management
- Monitor `/opt/airflow/dags/job9_op1_audio_daily_pipeline/` disk usage
- Consider implementing cleanup of old processed files
- Archive or compress older audio files if needed
- Monitor temporary directories used during processing

### Processing Time
- Large batches may take significant time
- Consider adjusting Airflow task timeout if needed
- Monitor memory usage during conversion

### Network Bandwidth
- Rsync transfers may be bandwidth-intensive
- Consider running during off-peak hours
- Use rsync compression (`-z` flag already enabled)

## Security Notes

- Secret key is stored in Airflow Variables as `AUDIO_SECRET_KEY` (encrypted at rest)
- SSH keys should be properly secured with appropriate permissions for both op1 and vidar servers
- Consider rotating SSH keys periodically
- Monitor access logs on both op1 and vidar servers
- Docker container access on op1 server is secured through SSH key authentication

## Maintenance

### Regular Tasks
- Monitor log file sizes and rotate if necessary
- Check disk space usage weekly
- Verify SSH key connectivity monthly
- Review and clean up old audio files quarterly

### Updates
- Test any script changes in development environment first
- Use version control for all pipeline scripts
- Document any configuration changes
- Backup critical data before major updates
