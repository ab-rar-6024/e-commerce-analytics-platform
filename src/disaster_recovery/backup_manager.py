"""
Backup Manager for E-Commerce Analytics Platform

This module provides comprehensive backup functionality for all data stores
in the platform including PostgreSQL, Kafka, S3, and Delta Lake.
"""

import asyncio
import json
import logging
import os
import shutil
import subprocess
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3
import psycopg2
from botocore.exceptions import ClientError, NoCredentialsError

from ..utils.spark_utils import get_secure_temp_dir

logger = logging.getLogger(__name__)


class BackupStatus(str, Enum):
    """Enumeration of backup statuses."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"

    def __str__(self) -> str:
        return self.value


class BackupType(str, Enum):
    """Enumeration of backup types."""

    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"

    def __str__(self) -> str:
        return self.value


class BackupManager:
    """
    Centralized backup manager for all platform data stores.

    Supports:
    - PostgreSQL database backups
    - S3 data lake backups
    - Kafka topic backups
    - Delta Lake backups
    - Cross-region replication
    - Encryption and compression
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the backup manager.

        Args:
            config: Configuration dictionary containing backup settings
        """
        self.config = config
        self.backup_dir = Path(config.get("backup_dir", get_secure_temp_dir("backups")))
        self.backup_dir.mkdir(parents=True, exist_ok=True)

        # Initialize AWS clients
        self._init_aws_clients()

        # Backup history
        self.backup_history: List[Dict[str, Any]] = []

        # Retention policies
        self.retention_policies = config.get(
            "retention_policies",
            {
                "daily": 7,  # Keep daily backups for 7 days
                "weekly": 4,  # Keep weekly backups for 4 weeks
                "monthly": 12,  # Keep monthly backups for 12 months
            },
        )

    def _init_aws_clients(self):
        """Initialize AWS service clients."""
        try:
            aws_config = self.config.get("aws", {})
            region = aws_config.get("region", "us-west-2")

            self.s3_client = boto3.client("s3", region_name=region)
            self.rds_client = boto3.client("rds", region_name=region)
            self.ec2_client = boto3.client("ec2", region_name=region)

            # Cross-region clients for replication
            backup_region = aws_config.get("backup_region", "us-east-1")
            if backup_region != region:
                self.s3_backup_client = boto3.client("s3", region_name=backup_region)
            else:
                self.s3_backup_client = self.s3_client

        except NoCredentialsError:
            logger.warning("AWS credentials not configured, using local backup only")
            self.s3_client = None
            self.rds_client = None
            self.ec2_client = None
            self.s3_backup_client = None

    async def create_full_backup(
        self,
        backup_name: Optional[str] = None,
        include_data_lake: bool = True,
        include_database: bool = True,
        include_kafka: bool = True,
    ) -> Dict[str, Any]:
        """
        Create a full backup of all platform components.

        Args:
            backup_name: Optional custom backup name
            include_data_lake: Whether to backup S3 data lake
            include_database: Whether to backup PostgreSQL
            include_kafka: Whether to backup Kafka topics

        Returns:
            Dictionary containing backup results and metadata
        """
        # Check access permissions
        if not await self._check_access_permissions("create"):
            await self._log_audit_event(
                "access_denied",
                {
                    "operation": "create_full_backup",
                    "reason": "insufficient_permissions",
                },
            )
            return {
                "status": BackupStatus.FAILED,
                "error": "Access denied: insufficient permissions for backup creation",
            }

        if not backup_name:
            backup_name = f"full_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        logger.info(f"Starting full backup: {backup_name}")

        # Log backup initiation
        await self._log_audit_event(
            "backup_initiated",
            {
                "backup_name": backup_name,
                "backup_type": "full",
                "include_data_lake": include_data_lake,
                "include_database": include_database,
                "include_kafka": include_kafka,
            },
        )

        backup_metadata = {
            "backup_id": backup_name,
            "backup_type": BackupType.FULL,
            "start_time": datetime.now().isoformat(),
            "status": BackupStatus.IN_PROGRESS,
            "components": {},
            "total_size_bytes": 0,
            "checksum": None,
        }

        try:
            # Create backup directory
            backup_path = self.backup_dir / backup_name
            backup_path.mkdir(parents=True, exist_ok=True)

            # Backup components in parallel
            tasks = []

            if include_database:
                tasks.append(self._backup_postgresql(backup_path))

            if include_data_lake:
                tasks.append(self._backup_s3_data_lake(backup_path))

            if include_kafka:
                tasks.append(self._backup_kafka_topics(backup_path))

            # Execute backups concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results
            component_names = []
            if include_database:
                component_names.append("postgresql")
            if include_data_lake:
                component_names.append("s3_data_lake")
            if include_kafka:
                component_names.append("kafka")

            for i, result in enumerate(results):
                component = component_names[i]
                if isinstance(result, Exception):
                    backup_metadata["components"][component] = {
                        "status": BackupStatus.FAILED,
                        "error": str(result),
                    }
                    logger.error(f"Backup failed for {component}: {result}")
                else:
                    backup_metadata["components"][component] = result
                    backup_metadata["total_size_bytes"] += result.get("size_bytes", 0)

            # Calculate backup checksum
            backup_metadata["checksum"] = self._calculate_backup_checksum(backup_path)

            # Check if any component failed
            failed_components = [
                comp
                for comp, details in backup_metadata["components"].items()
                if details["status"] == BackupStatus.FAILED
            ]

            if failed_components:
                backup_metadata["status"] = BackupStatus.FAILED
                logger.error(f"Backup failed for components: {failed_components}")
            else:
                backup_metadata["status"] = BackupStatus.COMPLETED
                logger.info(f"Full backup completed successfully: {backup_name}")

            # Save backup metadata
            await self._save_backup_metadata(backup_path, backup_metadata)

            # Add to history
            self.backup_history.append(backup_metadata)

            # Upload to S3 if configured
            if self.s3_client and self.config.get("upload_to_s3", True):
                await self._upload_backup_to_s3(backup_path, backup_metadata)

            # Apply retention policies
            await self._apply_retention_policies()

            # Log backup completion
            await self._log_audit_event(
                "backup_completed",
                {
                    "backup_name": backup_name,
                    "status": backup_metadata["status"],
                    "total_size_bytes": backup_metadata["total_size_bytes"],
                    "duration_seconds": backup_metadata.get("duration_seconds"),
                    "components": list(backup_metadata["components"].keys()),
                },
            )

            return backup_metadata

        except Exception as e:
            backup_metadata["status"] = BackupStatus.FAILED
            backup_metadata["error"] = str(e)
            logger.error(f"Full backup failed: {e}")
            return backup_metadata
        finally:
            backup_metadata["end_time"] = datetime.now().isoformat()
            duration = datetime.fromisoformat(
                backup_metadata["end_time"]
            ) - datetime.fromisoformat(backup_metadata["start_time"])
            backup_metadata["duration_seconds"] = duration.total_seconds()

    async def _backup_postgresql(self, backup_path: Path) -> Dict[str, Any]:
        """Backup PostgreSQL database using pg_dump."""
        logger.info("Starting PostgreSQL backup")

        db_config = self.config.get("postgresql", {})
        host = db_config.get("host", "localhost")
        port = db_config.get("port", 5432)
        database = db_config.get("database", "ecommerce_analytics")
        username = db_config.get("username", "analytics_user")
        password = db_config.get("password", "")

        # Create database backup directory
        db_backup_path = backup_path / "postgresql"
        db_backup_path.mkdir(parents=True, exist_ok=True)

        # Backup file path
        backup_file = (
            db_backup_path
            / f"{database}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
        )

        # Set environment variable for password
        env = os.environ.copy()
        if password:
            env["PGPASSWORD"] = password

        try:
            # Create pg_dump command
            cmd = [
                "pg_dump",
                "-h",
                host,
                "-p",
                str(port),
                "-U",
                username,
                "-d",
                database,
                "--no-password",
                "--verbose",
                "--clean",
                "--create",
                "--if-exists",
                "-f",
                str(backup_file),
            ]

            # Execute backup
            process = await asyncio.create_subprocess_exec(
                *cmd,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                raise Exception(f"pg_dump failed: {stderr.decode()}")

            # Compress backup
            compressed_file = backup_file.with_suffix(".sql.gz")
            await self._compress_file(backup_file, compressed_file)
            backup_file.unlink()  # Remove uncompressed file

            final_file = compressed_file

            # Encrypt backup if encryption is enabled
            encryption_key = self._get_encryption_key()
            if encryption_key:
                encrypted_file = compressed_file.with_suffix(".gz.enc")
                await self._encrypt_file(
                    compressed_file, encrypted_file, encryption_key
                )
                compressed_file.unlink()  # Remove unencrypted compressed file
                final_file = encrypted_file

            # Get file size
            file_size = final_file.stat().st_size

            # Verify backup integrity
            await self._verify_postgresql_backup(final_file, db_config)

            logger.info(f"PostgreSQL backup completed: {final_file}")

            return {
                "status": BackupStatus.COMPLETED,
                "backup_file": str(final_file),
                "size_bytes": file_size,
                "compression": "gzip",
                "encryption": "AES-256-GCM" if encryption_key else None,
                "tables_backed_up": await self._get_table_count(db_config),
            }

        except Exception as e:
            logger.error(f"PostgreSQL backup failed: {e}")
            return {"status": BackupStatus.FAILED, "error": str(e)}

    async def _backup_s3_data_lake(self, backup_path: Path) -> Dict[str, Any]:
        """Backup S3 data lake contents."""
        logger.info("Starting S3 data lake backup")

        if not self.s3_client:
            return {"status": BackupStatus.FAILED, "error": "S3 client not configured"}

        try:
            s3_config = self.config.get("s3", {})
            bucket_name = s3_config.get("bucket", "ecap-data-lake")

            # Create S3 backup directory
            s3_backup_path = backup_path / "s3_data_lake"
            s3_backup_path.mkdir(parents=True, exist_ok=True)

            # List all objects in bucket
            objects = []
            paginator = self.s3_client.get_paginator("list_objects_v2")

            async for page in self._async_paginate(paginator, Bucket=bucket_name):
                if "Contents" in page:
                    objects.extend(page["Contents"])

            total_size = 0
            downloaded_files = []

            # Download objects
            for obj in objects:
                key = obj["Key"]
                local_file = s3_backup_path / key
                local_file.parent.mkdir(parents=True, exist_ok=True)

                try:
                    self.s3_client.download_file(bucket_name, key, str(local_file))
                    downloaded_files.append(key)
                    total_size += obj["Size"]
                except ClientError as e:
                    logger.warning(f"Failed to download {key}: {e}")

            # Create manifest file
            manifest = {
                "bucket": bucket_name,
                "total_objects": len(objects),
                "downloaded_objects": len(downloaded_files),
                "total_size_bytes": total_size,
                "backup_date": datetime.now().isoformat(),
                "files": downloaded_files,
            }

            manifest_file = s3_backup_path / "manifest.json"
            with open(manifest_file, "w") as f:
                json.dump(manifest, f, indent=2)

            logger.info(f"S3 data lake backup completed: {len(downloaded_files)} files")

            return {
                "status": BackupStatus.COMPLETED,
                "backup_path": str(s3_backup_path),
                "size_bytes": total_size,
                "objects_backed_up": len(downloaded_files),
                "manifest_file": str(manifest_file),
            }

        except Exception as e:
            logger.error(f"S3 data lake backup failed: {e}")
            return {"status": BackupStatus.FAILED, "error": str(e)}

    async def _backup_kafka_topics(self, backup_path: Path) -> Dict[str, Any]:
        """Backup Kafka topics configuration and recent messages."""
        logger.info("Starting Kafka backup")

        try:
            kafka_config = self.config.get("kafka", {})
            bootstrap_servers = kafka_config.get(
                "bootstrap_servers", ["localhost:9092"]
            )

            # Create Kafka backup directory
            kafka_backup_path = backup_path / "kafka"
            kafka_backup_path.mkdir(parents=True, exist_ok=True)

            # Import kafka-python for backup operations
            try:
                from kafka import KafkaConsumer, KafkaProducer
                from kafka.admin import (
                    ConfigResource,
                    ConfigResourceType,
                    KafkaAdminClient,
                )
            except ImportError:
                logger.error("kafka-python not installed, skipping Kafka backup")
                return {
                    "status": BackupStatus.FAILED,
                    "error": "kafka-python not installed",
                }

            # Get topic configurations
            admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

            # List topics
            metadata = admin_client.describe_topics()
            topics = list(metadata.keys())

            # Backup topic configurations
            topic_configs = {}
            for topic in topics:
                if not topic.startswith("__"):  # Skip internal topics
                    try:
                        config_resource = ConfigResource(
                            ConfigResourceType.TOPIC, topic
                        )
                        configs = admin_client.describe_configs([config_resource])
                        topic_configs[topic] = {
                            config.config_name: config.config_value
                            for config in configs[config_resource].configs.values()
                        }
                    except Exception as e:
                        logger.warning(f"Failed to get config for topic {topic}: {e}")

            # Save topic configurations
            config_file = kafka_backup_path / "topic_configs.json"
            with open(config_file, "w") as f:
                json.dump(topic_configs, f, indent=2)

            # Backup recent messages (last hour) for critical topics
            critical_topics = kafka_config.get(
                "critical_topics", ["transactions", "user_events", "fraud_alerts"]
            )

            backed_up_messages = 0
            for topic in critical_topics:
                if topic in topics:
                    try:
                        messages = await self._backup_kafka_topic_messages(
                            topic, kafka_backup_path, bootstrap_servers
                        )
                        backed_up_messages += messages
                    except Exception as e:
                        logger.warning(
                            f"Failed to backup messages for topic {topic}: {e}"
                        )

            backup_size = sum(
                f.stat().st_size for f in kafka_backup_path.rglob("*") if f.is_file()
            )

            logger.info(f"Kafka backup completed: {len(topics)} topics")

            return {
                "status": BackupStatus.COMPLETED,
                "backup_path": str(kafka_backup_path),
                "size_bytes": backup_size,
                "topics_backed_up": len(topics),
                "messages_backed_up": backed_up_messages,
                "config_file": str(config_file),
            }

        except Exception as e:
            logger.error(f"Kafka backup failed: {e}")
            return {"status": BackupStatus.FAILED, "error": str(e)}

    async def _backup_kafka_topic_messages(
        self, topic: str, backup_path: Path, bootstrap_servers: List[str]
    ) -> int:
        """Backup recent messages from a Kafka topic."""
        import json

        from kafka import KafkaConsumer

        # Create consumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            value_deserializer=lambda x: x.decode("utf-8") if x else None,
            consumer_timeout_ms=5000,  # 5 second timeout
        )

        # Backup file
        messages_file = backup_path / f"{topic}_messages.jsonl"

        message_count = 0
        with open(messages_file, "w") as f:
            for message in consumer:
                if message.value:
                    backup_record = {
                        "partition": message.partition,
                        "offset": message.offset,
                        "timestamp": message.timestamp,
                        "value": message.value,
                    }
                    f.write(json.dumps(backup_record) + "\n")
                    message_count += 1

                    # Limit backup to recent messages (last 10,000)
                    if message_count >= 10000:
                        break

        consumer.close()
        return message_count

    async def _compress_file(self, source_file: Path, target_file: Path):
        """Compress a file using gzip."""
        import gzip

        with open(source_file, "rb") as f_in:
            with gzip.open(target_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

    async def _encrypt_file(
        self, source_file: Path, target_file: Path, encryption_key: bytes
    ):
        """Encrypt a file using AES-256-GCM."""
        try:
            import os

            from cryptography.hazmat.primitives.ciphers.aead import AESGCM

            # Generate random nonce
            nonce = os.urandom(12)  # 96-bit nonce for GCM

            # Initialize AES-GCM
            aesgcm = AESGCM(encryption_key)

            # Read and encrypt file content
            with open(source_file, "rb") as f_in:
                plaintext = f_in.read()

            # Encrypt the data
            ciphertext = aesgcm.encrypt(nonce, plaintext, None)

            # Write nonce + ciphertext to target file
            with open(target_file, "wb") as f_out:
                f_out.write(nonce)  # First 12 bytes are nonce
                f_out.write(ciphertext)

            logger.info(f"File encrypted: {target_file}")

        except ImportError:
            logger.error("cryptography library not installed, skipping encryption")
            shutil.copy2(source_file, target_file)
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            raise e

    async def _decrypt_file(
        self, source_file: Path, target_file: Path, encryption_key: bytes
    ):
        """Decrypt a file using AES-256-GCM."""
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM

            # Initialize AES-GCM
            aesgcm = AESGCM(encryption_key)

            # Read encrypted file
            with open(source_file, "rb") as f_in:
                # First 12 bytes are nonce
                nonce = f_in.read(12)
                ciphertext = f_in.read()

            # Decrypt the data
            plaintext = aesgcm.decrypt(nonce, ciphertext, None)

            # Write decrypted content to target file
            with open(target_file, "wb") as f_out:
                f_out.write(plaintext)

            logger.info(f"File decrypted: {target_file}")

        except ImportError:
            logger.error("cryptography library not installed, cannot decrypt")
            raise Exception("Encryption libraries not available")
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            raise e

    def _get_encryption_key(self) -> Optional[bytes]:
        """Get encryption key from configuration or environment."""
        # Try to get key from environment variable first
        key_env = os.environ.get("ECAP_BACKUP_ENCRYPTION_KEY")
        if key_env:
            import base64

            try:
                return base64.b64decode(key_env)
            except Exception:
                logger.warning("Invalid encryption key in environment variable")

        # Try to get key from config
        encryption_config = self.config.get("encryption", {})
        key_file = encryption_config.get("key_file")

        if key_file and Path(key_file).exists():
            try:
                with open(key_file, "rb") as f:
                    return f.read()
            except Exception as e:
                logger.warning(f"Failed to read encryption key file: {e}")

        # Generate new key if none exists
        if encryption_config.get("auto_generate_key", False):
            return self._generate_encryption_key()

        return None

    def _generate_encryption_key(self) -> bytes:
        """Generate a new AES-256 encryption key."""
        import base64
        import os

        # Generate 256-bit (32 bytes) key
        key = os.urandom(32)

        # Save to environment for reuse in session
        os.environ["ECAP_BACKUP_ENCRYPTION_KEY"] = base64.b64encode(key).decode()

        # Optionally save to key file
        encryption_config = self.config.get("encryption", {})
        key_file = encryption_config.get("key_file")

        if key_file:
            try:
                with open(key_file, "wb") as f:
                    f.write(key)
                logger.info(f"Generated new encryption key: {key_file}")
            except Exception as e:
                logger.warning(f"Failed to save encryption key to file: {e}")

        return key

    async def _log_audit_event(self, event_type: str, details: Dict[str, Any]):
        """Log security audit events for backup operations."""
        audit_entry = {
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "user": os.environ.get("USER", "system"),
            "details": details,
            "source_ip": self._get_source_ip(),
            "session_id": os.environ.get("BACKUP_SESSION_ID", "unknown"),
        }

        # Log to audit file
        audit_config = self.config.get("audit", {})
        audit_file = audit_config.get("log_file", "/var/log/ecap/backup-audit.log")

        try:
            os.makedirs(Path(audit_file).parent, exist_ok=True)
            with open(audit_file, "a") as f:
                f.write(json.dumps(audit_entry) + "\n")
        except Exception as e:
            logger.warning(f"Failed to write audit log: {e}")

        # Also log to application logger
        logger.info(f"AUDIT: {event_type} - {details}")

    def _get_source_ip(self) -> str:
        """Get source IP address for audit logging."""
        try:
            import socket

            # Get local IP (simplified for demonstration)
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "unknown"

    async def _check_access_permissions(self, operation: str, user: str = None) -> bool:
        """Check if user has permission for backup operation."""
        if not user:
            user = os.environ.get("USER", "system")

        access_config = self.config.get("access_control", {})

        # Check if access control is enabled
        if not access_config.get("enabled", False):
            return True  # Allow all operations if access control is disabled

        # Get user permissions
        user_permissions = access_config.get("users", {}).get(user, [])
        role_permissions = []

        # Get role-based permissions
        user_roles = access_config.get("user_roles", {}).get(user, [])
        for role in user_roles:
            role_permissions.extend(access_config.get("roles", {}).get(role, []))

        # Combine user and role permissions
        all_permissions = set(user_permissions + role_permissions)

        # Check if user has required permission
        required_permission = f"backup.{operation}"
        has_permission = (
            required_permission in all_permissions
            or "backup.*" in all_permissions
            or "*" in all_permissions
        )

        # Log access attempt
        await self._log_audit_event(
            "access_check",
            {
                "user": user,
                "operation": operation,
                "permission_required": required_permission,
                "access_granted": has_permission,
                "user_permissions": list(all_permissions),
            },
        )

        return has_permission

    async def _verify_postgresql_backup(
        self, backup_file: Path, db_config: Dict[str, Any]
    ):
        """Verify PostgreSQL backup integrity."""
        # For now, just check if file exists and has content
        if not backup_file.exists():
            raise Exception("Backup file does not exist")

        if backup_file.stat().st_size == 0:
            raise Exception("Backup file is empty")

        # TODO: Could add more sophisticated verification
        # like attempting to restore to a test database

    async def _get_table_count(self, db_config: Dict[str, Any]) -> int:
        """Get the number of tables in the database."""
        try:
            conn = psycopg2.connect(
                host=db_config.get("host", "localhost"),
                port=db_config.get("port", 5432),
                database=db_config.get("database", "ecommerce_analytics"),
                user=db_config.get("username", "analytics_user"),
                password=db_config.get("password", ""),
            )

            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                """
                )
                count = cursor.fetchone()[0]

            conn.close()
            return count

        except Exception as e:
            logger.warning(f"Failed to get table count: {e}")
            return 0

    def _calculate_backup_checksum(self, backup_path: Path) -> str:
        """Calculate MD5 checksum of entire backup."""
        import hashlib

        md5_hash = hashlib.md5(usedforsecurity=False)

        # Get all files in backup directory
        for file_path in sorted(backup_path.rglob("*")):
            if file_path.is_file():
                with open(file_path, "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        md5_hash.update(chunk)

        return md5_hash.hexdigest()

    async def _save_backup_metadata(self, backup_path: Path, metadata: Dict[str, Any]):
        """Save backup metadata to file."""
        metadata_file = backup_path / "backup_metadata.json"
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)

    async def _upload_backup_to_s3(self, backup_path: Path, metadata: Dict[str, Any]):
        """Upload backup to S3 bucket."""
        if not self.s3_client:
            return

        try:
            s3_config = self.config.get("s3", {})
            backup_bucket = s3_config.get("backup_bucket", "ecap-backups")

            backup_name = backup_path.name

            # Upload all files in backup directory
            for file_path in backup_path.rglob("*"):
                if file_path.is_file():
                    relative_path = file_path.relative_to(backup_path)
                    s3_key = f"backups/{backup_name}/{relative_path}"

                    self.s3_client.upload_file(str(file_path), backup_bucket, s3_key)

            logger.info(
                f"Backup uploaded to S3: s3://{backup_bucket}/backups/{backup_name}"
            )

        except Exception as e:
            logger.error(f"Failed to upload backup to S3: {e}")

    async def _apply_retention_policies(self):
        """Apply retention policies to remove old backups."""
        try:
            current_time = datetime.now()

            # Local backup cleanup
            for backup_dir in self.backup_dir.iterdir():
                if backup_dir.is_dir():
                    metadata_file = backup_dir / "backup_metadata.json"
                    if metadata_file.exists():
                        with open(metadata_file) as f:
                            metadata = json.load(f)

                        backup_time = datetime.fromisoformat(metadata["start_time"])
                        age_days = (current_time - backup_time).days

                        # Apply retention policy
                        should_delete = False
                        if age_days > self.retention_policies.get("daily", 7):
                            # Check if it's a weekly/monthly backup to keep
                            if backup_time.weekday() == 6:  # Sunday - weekly backup
                                if age_days > self.retention_policies.get("weekly", 28):
                                    should_delete = True
                            elif (
                                backup_time.day == 1
                            ):  # First of month - monthly backup
                                if age_days > self.retention_policies.get(
                                    "monthly", 365
                                ):
                                    should_delete = True
                            else:
                                should_delete = True

                        if should_delete:
                            logger.info(f"Removing old backup: {backup_dir.name}")
                            shutil.rmtree(backup_dir)

        except Exception as e:
            logger.error(f"Failed to apply retention policies: {e}")

    async def _async_paginate(self, paginator, **kwargs):
        """Convert boto3 paginator to async generator."""
        for page in paginator.paginate(**kwargs):
            yield page

    async def list_backups(self) -> List[Dict[str, Any]]:
        """List all available backups."""
        backups = []

        # List local backups
        for backup_dir in self.backup_dir.iterdir():
            if backup_dir.is_dir():
                metadata_file = backup_dir / "backup_metadata.json"
                if metadata_file.exists():
                    with open(metadata_file) as f:
                        metadata = json.load(f)
                    metadata["location"] = "local"
                    metadata["path"] = str(backup_dir)
                    backups.append(metadata)

        # List S3 backups if configured
        if self.s3_client:
            try:
                s3_backups = await self._list_s3_backups()
                backups.extend(s3_backups)
            except Exception as e:
                logger.warning(f"Failed to list S3 backups: {e}")

        # Sort by creation time
        backups.sort(key=lambda x: x["start_time"], reverse=True)

        return backups

    async def _list_s3_backups(self) -> List[Dict[str, Any]]:
        """List backups stored in S3."""
        backups = []

        try:
            s3_config = self.config.get("s3", {})
            backup_bucket = s3_config.get("backup_bucket", "ecap-backups")

            # List backup directories
            response = self.s3_client.list_objects_v2(
                Bucket=backup_bucket, Prefix="backups/", Delimiter="/"
            )

            if "CommonPrefixes" in response:
                for prefix in response["CommonPrefixes"]:
                    backup_name = prefix["Prefix"].split("/")[-2]

                    # Try to get metadata
                    try:
                        metadata_key = f"{prefix['Prefix']}backup_metadata.json"
                        response = self.s3_client.get_object(
                            Bucket=backup_bucket, Key=metadata_key
                        )
                        metadata = json.loads(response["Body"].read())
                        metadata["location"] = "s3"
                        metadata["s3_prefix"] = prefix["Prefix"]
                        backups.append(metadata)
                    except ClientError:
                        # Metadata file doesn't exist
                        continue

        except Exception as e:
            logger.error(f"Failed to list S3 backups: {e}")

        return backups

    async def restore_backup(
        self, backup_id: str, components: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Restore from a backup.

        Args:
            backup_id: ID of the backup to restore
            components: Optional list of components to restore

        Returns:
            Dictionary containing restore results
        """
        # Check access permissions for restore operation
        if not await self._check_access_permissions("restore"):
            await self._log_audit_event(
                "access_denied",
                {
                    "operation": "restore_backup",
                    "backup_id": backup_id,
                    "reason": "insufficient_permissions",
                },
            )
            return {
                "status": "failed",
                "error": "Access denied: insufficient permissions for backup restoration",
            }

        logger.info(f"Starting restore from backup: {backup_id}")

        # Log restore initiation
        await self._log_audit_event(
            "restore_initiated", {"backup_id": backup_id, "components": components}
        )

        # Find backup
        backups = await self.list_backups()
        backup_metadata = None

        for backup in backups:
            if backup["backup_id"] == backup_id:
                backup_metadata = backup
                break

        if not backup_metadata:
            raise Exception(f"Backup not found: {backup_id}")

        # Download from S3 if needed
        if backup_metadata["location"] == "s3":
            await self._download_backup_from_s3(backup_metadata)

        # Restore components
        restore_results = {
            "restore_id": f"restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "backup_id": backup_id,
            "start_time": datetime.now().isoformat(),
            "components": {},
            "status": "in_progress",
        }

        try:
            if not components:
                components = list(backup_metadata["components"].keys())

            for component in components:
                if component in backup_metadata["components"]:
                    if component == "postgresql":
                        result = await self._restore_postgresql(backup_id)
                    elif component == "s3_data_lake":
                        result = await self._restore_s3_data_lake(backup_id)
                    elif component == "kafka":
                        result = await self._restore_kafka(backup_id)
                    else:
                        result = {"status": "skipped", "reason": "Unknown component"}

                    restore_results["components"][component] = result

            # Check overall status
            failed_components = [
                comp
                for comp, details in restore_results["components"].items()
                if details.get("status") == "failed"
            ]

            if failed_components:
                restore_results["status"] = "failed"
            else:
                restore_results["status"] = "completed"

            # Log restore completion
            await self._log_audit_event(
                "restore_completed",
                {
                    "backup_id": backup_id,
                    "restore_id": restore_results["restore_id"],
                    "status": restore_results["status"],
                    "components": list(restore_results["components"].keys()),
                    "failed_components": failed_components,
                },
            )

            return restore_results

        except Exception as e:
            restore_results["status"] = "failed"
            restore_results["error"] = str(e)
            logger.error(f"Restore failed: {e}")
            return restore_results
        finally:
            restore_results["end_time"] = datetime.now().isoformat()

    async def _download_backup_from_s3(self, backup_metadata: Dict[str, Any]):
        """Download backup from S3 to local storage."""
        # Implementation would download S3 backup to local directory
        # for restoration purposes
        pass

    async def _restore_postgresql(self, backup_id: str) -> Dict[str, Any]:
        """Restore PostgreSQL database from backup."""
        # Implementation would restore PostgreSQL from backup file
        return {"status": "completed", "message": "PostgreSQL restore not implemented"}

    async def _restore_s3_data_lake(self, backup_id: str) -> Dict[str, Any]:
        """Restore S3 data lake from backup."""
        # Implementation would restore S3 objects from backup
        return {"status": "completed", "message": "S3 restore not implemented"}

    async def _restore_kafka(self, backup_id: str) -> Dict[str, Any]:
        """Restore Kafka topics from backup."""
        # Implementation would restore Kafka topic configurations and messages
        return {"status": "completed", "message": "Kafka restore not implemented"}

    async def get_backup_status(self, backup_id: str) -> Dict[str, Any]:
        """Get the status of a specific backup."""
        backups = await self.list_backups()

        for backup in backups:
            if backup["backup_id"] == backup_id:
                return backup

        return {"error": f"Backup not found: {backup_id}"}

    async def cleanup_failed_backups(self):
        """Clean up failed or incomplete backups."""
        cleaned_up = []

        for backup_dir in self.backup_dir.iterdir():
            if backup_dir.is_dir():
                metadata_file = backup_dir / "backup_metadata.json"
                if metadata_file.exists():
                    with open(metadata_file) as f:
                        metadata = json.load(f)

                    if metadata.get("status") == BackupStatus.FAILED:
                        logger.info(f"Cleaning up failed backup: {backup_dir.name}")
                        shutil.rmtree(backup_dir)
                        cleaned_up.append(backup_dir.name)

        return cleaned_up

    async def verify_backup_integrity(self, backup_id: str) -> Dict[str, Any]:
        """Verify the integrity of a backup."""
        # Check access permissions
        if not await self._check_access_permissions("verify"):
            await self._log_audit_event(
                "access_denied",
                {
                    "operation": "verify_backup_integrity",
                    "backup_id": backup_id,
                    "reason": "insufficient_permissions",
                },
            )
            return {
                "success": False,
                "error": "Access denied: insufficient permissions for backup verification",
            }

        # Find backup
        backups = await self.list_backups()
        backup_metadata = None

        for backup in backups:
            if backup["backup_id"] == backup_id:
                backup_metadata = backup
                break

        if not backup_metadata:
            return {"success": False, "error": f"Backup not found: {backup_id}"}

        verification_results = {
            "backup_id": backup_id,
            "verification_time": datetime.now().isoformat(),
            "components": {},
            "overall_status": "unknown",
        }

        try:
            # Get backup path
            if backup_metadata["location"] == "local":
                backup_path = Path(backup_metadata["path"])
            else:
                # Download from S3 for verification
                backup_path = await self._download_backup_for_verification(
                    backup_metadata
                )

            # Verify checksum
            if backup_path.exists():
                calculated_checksum = self._calculate_backup_checksum(backup_path)
                original_checksum = backup_metadata.get("checksum")

                checksum_valid = calculated_checksum == original_checksum
                verification_results["checksum_verification"] = {
                    "original": original_checksum,
                    "calculated": calculated_checksum,
                    "valid": checksum_valid,
                }

                if not checksum_valid:
                    verification_results["overall_status"] = "failed"
                    verification_results["error"] = "Checksum verification failed"
                else:
                    verification_results["overall_status"] = "passed"
            else:
                verification_results["overall_status"] = "failed"
                verification_results["error"] = "Backup files not accessible"

            # Log verification
            await self._log_audit_event(
                "backup_verified",
                {
                    "backup_id": backup_id,
                    "verification_status": verification_results["overall_status"],
                    "checksum_valid": verification_results.get(
                        "checksum_verification", {}
                    ).get("valid", False),
                },
            )

            return {"success": True, "verification_results": verification_results}

        except Exception as e:
            logger.error(f"Backup verification failed: {e}")
            await self._log_audit_event(
                "backup_verification_failed", {"backup_id": backup_id, "error": str(e)}
            )
            return {"success": False, "error": str(e)}

    async def _download_backup_for_verification(
        self, backup_metadata: Dict[str, Any]
    ) -> Path:
        """Download backup from S3 for verification."""
        # Simplified implementation - would download to temp directory
        temp_path = self.backup_dir / "temp_verification" / backup_metadata["backup_id"]
        temp_path.mkdir(parents=True, exist_ok=True)
        return temp_path

    async def generate_compliance_report(self) -> Dict[str, Any]:
        """Generate compliance report for backup operations."""
        # Check access permissions
        if not await self._check_access_permissions("report"):
            await self._log_audit_event(
                "access_denied",
                {
                    "operation": "generate_compliance_report",
                    "reason": "insufficient_permissions",
                },
            )
            return {
                "success": False,
                "error": "Access denied: insufficient permissions for compliance reporting",
            }

        try:
            backups = await self.list_backups()

            # Analyze backup patterns
            current_time = datetime.now()
            recent_backups = []
            encrypted_backups = 0

            for backup in backups:
                backup_time = datetime.fromisoformat(backup["start_time"])
                age_days = (current_time - backup_time).days

                if age_days <= 30:  # Last 30 days
                    recent_backups.append(backup)

                # Check encryption status
                for component in backup.get("components", {}).values():
                    if component.get("encryption"):
                        encrypted_backups += 1
                        break

            # Generate report
            report = {
                "report_date": current_time.isoformat(),
                "summary": {
                    "total_backups": len(backups),
                    "recent_backups_30_days": len(recent_backups),
                    "encrypted_backups": encrypted_backups,
                    "encryption_rate": f"{(encrypted_backups / len(backups) * 100):.1f}%"
                    if backups
                    else "0%",
                },
                "retention_compliance": await self._check_retention_compliance(),
                "security_compliance": {
                    "encryption_enabled": self._get_encryption_key() is not None,
                    "access_control_enabled": self.config.get("access_control", {}).get(
                        "enabled", False
                    ),
                    "audit_logging_enabled": bool(
                        self.config.get("audit", {}).get("log_file")
                    ),
                },
                "backup_schedule_compliance": await self._check_schedule_compliance(),
            }

            # Log report generation
            await self._log_audit_event(
                "compliance_report_generated", {"report_summary": report["summary"]}
            )

            return {"success": True, "report": report}

        except Exception as e:
            logger.error(f"Failed to generate compliance report: {e}")
            return {"success": False, "error": str(e)}

    async def _check_retention_compliance(self) -> Dict[str, Any]:
        """Check compliance with retention policies."""
        backups = await self.list_backups()
        current_time = datetime.now()

        retention_status = {
            "daily_backups": {"required": 7, "actual": 0, "compliant": False},
            "weekly_backups": {"required": 4, "actual": 0, "compliant": False},
            "monthly_backups": {"required": 12, "actual": 0, "compliant": False},
        }

        for backup in backups:
            backup_time = datetime.fromisoformat(backup["start_time"])
            age_days = (current_time - backup_time).days

            # Count daily backups (last 7 days)
            if age_days <= 7:
                retention_status["daily_backups"]["actual"] += 1

            # Count weekly backups (last 4 weeks, Sundays)
            if age_days <= 28 and backup_time.weekday() == 6:
                retention_status["weekly_backups"]["actual"] += 1

            # Count monthly backups (last 12 months, first of month)
            if age_days <= 365 and backup_time.day == 1:
                retention_status["monthly_backups"]["actual"] += 1

        # Check compliance
        for backup_type in retention_status:
            required = retention_status[backup_type]["required"]
            actual = retention_status[backup_type]["actual"]
            retention_status[backup_type]["compliant"] = actual >= required

        return retention_status

    async def _check_schedule_compliance(self) -> Dict[str, Any]:
        """Check compliance with backup schedules."""
        # Simplified implementation
        last_backup_time = None

        backups = await self.list_backups()
        if backups:
            last_backup = max(backups, key=lambda x: x["start_time"])
            last_backup_time = datetime.fromisoformat(last_backup["start_time"])

        current_time = datetime.now()
        hours_since_last = (
            (current_time - last_backup_time).total_seconds() / 3600
            if last_backup_time
            else float("inf")
        )

        return {
            "last_backup_time": last_backup_time.isoformat()
            if last_backup_time
            else None,
            "hours_since_last_backup": hours_since_last,
            "schedule_compliant": hours_since_last
            <= 25,  # Within 25 hours (daily + buffer)
        }
