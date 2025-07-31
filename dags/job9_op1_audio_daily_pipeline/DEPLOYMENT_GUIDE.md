# Job9 Audio Pipeline Deployment Guide

## Overview
Job9 is an Airflow DAG that processes audio files from the op1 server and syncs them to vidar.research.cs.dal.ca. The pipeline consists of three main steps:

1. **Download**: Downloads encrypted .aac files from op1 server
2. **Decrypt/Convert**: Decrypts and converts files to WAV format
3. **Sync**: Uploads WAV files to nathan@vidar.research.cs.dal.ca:/home/audio/

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
airflow variables set op1_audio_secret_key "your_secret_key_here"
```

### 3. SSH Key Authentication
Set up SSH key authentication for rsync to work without password prompts:

```bash
# Generate SSH key if not exists
ssh-keygen -t rsa -b 4096 -C "prositadmin@prosit_scheduler"

# Copy public key to vidar server
ssh-copy-id nathan@vidar.research.cs.dal.ca

# Test connection
ssh nathan@vidar.research.cs.dal.ca "echo 'SSH connection successful'"
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
- **Download Script**: `/home/prositadmin/apps/prosit_scheduler/dags/job9_audio_daily_pipeline/op1_download_audio_files.sh`
- **Conversion Script**: `/home/prositadmin/apps/prosit_scheduler/dags/job9_audio_daily_pipeline/op1_decrypt_convert_to_wav.py`
- **Working Directory**: `/home/prositadmin/audio_processing/audio_combined/`
- **Sync Destination**: `nathan@vidar.research.cs.dal.ca:/home/audio/`

## Deployment Steps

### 1. Verify DAG Syntax
```bash
cd /home/prositadmin/apps/prosit_scheduler/dags/job9_audio_daily_pipeline/
python3 -m py_compile audio_daily_pipeline.py
```

### 2. Test Individual Components
```bash
# Test download script syntax
bash -n op1_download_audio_files.sh

# Test conversion script syntax
python3 -m py_compile op1_decrypt_convert_to_wav.py

# Run comprehensive test
python3 test_job9_pipeline.py
```

### 3. Enable DAG in Airflow
1. Copy DAG files to Airflow DAGs directory
2. Refresh Airflow UI
3. Enable the `audio_pipeline_op1_to_vidar` DAG
4. Monitor first execution

## Monitoring

### Log Files
- **Conversion Log**: `/home/prositadmin/audio_processing/op1_decrypt_convert_to_wav.log`
- **Airflow Task Logs**: Available in Airflow UI

### Key Metrics to Monitor
- Number of files downloaded
- Decryption success rate
- Conversion success rate
- Sync completion status
- Disk space usage in `/home/prositadmin/audio_processing/`

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
- Verify `op1_audio_secret_key` Airflow variable is set correctly
- Check if source files are properly encrypted
- Verify file format matches expected JSON structure

#### 4. Conversion Failures
**Error**: `ffmpeg` conversion errors
**Solutions**:
- Ensure `ffmpeg` is installed
- Check if decrypted files are valid audio format
- Verify sufficient disk space

#### 5. Sync Failures
**Error**: `rsync` connection issues
**Solutions**:
- Test SSH connection manually
- Verify destination directory exists on vidar server
- Check network connectivity

### Manual Testing Commands
```bash
# Test SSH connection
ssh nathan@vidar.research.cs.dal.ca "ls -la /home/audio/"

# Test rsync (dry run)
rsync -avz --dry-run /home/prositadmin/audio_processing/audio_combined/wav/ nathan@vidar.research.cs.dal.ca:/home/audio/

# Check disk space
df -h /home/prositadmin/audio_processing/

# View recent logs
tail -f /home/prositadmin/audio_processing/op1_decrypt_convert_to_wav.log
```

## Performance Considerations

### Disk Space Management
- Monitor `/home/prositadmin/audio_processing/` disk usage
- Consider implementing cleanup of old processed files
- Archive or compress older audio files if needed

### Processing Time
- Large batches may take significant time
- Consider adjusting Airflow task timeout if needed
- Monitor memory usage during conversion

### Network Bandwidth
- Rsync transfers may be bandwidth-intensive
- Consider running during off-peak hours
- Use rsync compression (`-z` flag already enabled)

## Security Notes

- Secret key is stored in Airflow Variables (encrypted at rest)
- SSH keys should be properly secured with appropriate permissions
- Consider rotating SSH keys periodically
- Monitor access logs on vidar server

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
