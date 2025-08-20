#!/usr/bin/env python3
import os
import subprocess
import boto3
from datetime import datetime
import schedule
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class RedisBackup:
    def __init__(self):
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = os.getenv('REDIS_PORT', '6379')
        self.redis_password = os.getenv('REDIS_PASSWORD')
        self.s3_bucket = os.getenv('S3_BUCKET')
        self.s3_prefix = os.getenv('S3_PREFIX', 'redis-backups')
        self.cron_schedule = os.getenv('CRON_SCHEDULE', '0 * * * *')  # hourly
        
        # S3 client setup
        self.s3 = boto3.client(
            's3',
            endpoint_url=os.getenv('S3_ENDPOINT'),
            aws_access_key_id=os.getenv('S3_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('S3_SECRET_KEY'),
            region_name=os.getenv('S3_REGION', 'us-east-1')
        )
    
    def backup_redis(self):
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        dump_file = f'/tmp/dump-{timestamp}.rdb'
        
        try:
            # Create Redis dump
            logger.info(f"Creating Redis backup from {self.redis_host}:{self.redis_port}")
            
            # Build redis-cli command with auth
            cmd = ['redis-cli', '-h', self.redis_host, '-p', self.redis_port]
            if self.redis_password:
                cmd.extend(['-a', self.redis_password])
            cmd.extend(['--rdb', dump_file])
            
            subprocess.run(cmd, check=True)
            
            # Upload to S3
            s3_key = f"{self.s3_prefix}/dump-{timestamp}.rdb"
            logger.info(f"Uploading to s3://{self.s3_bucket}/{s3_key}")
            
            self.s3.upload_file(dump_file, self.s3_bucket, s3_key)
            
            # Cleanup local file
            os.remove(dump_file)
            logger.info("Backup completed successfully")
            
        except Exception as e:
            logger.error(f"Backup failed: {e}")
    
    def run(self):
        logger.info(f"Starting Redis backup service (schedule: {self.cron_schedule})")
        
        # Parse cron schedule (simplified - just support hourly/daily for now)
        if self.cron_schedule == '0 * * * *':  # hourly
            schedule.every().hour.do(self.backup_redis)
        elif self.cron_schedule == '0 0 * * *':  # daily
            schedule.every().day.at("00:00").do(self.backup_redis)
        else:
            # Default to hourly if can't parse
            schedule.every().hour.do(self.backup_redis)
        
        # Run initial backup
        self.backup_redis()
        
        # Keep running
        while True:
            schedule.run_pending()
            time.sleep(60)

if __name__ == '__main__':
    backup = RedisBackup()
    backup.run()
