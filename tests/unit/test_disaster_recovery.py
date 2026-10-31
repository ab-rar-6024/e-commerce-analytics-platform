"""
Unit tests for disaster recovery components.
"""

import asyncio
import json
import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from src.disaster_recovery.backup_automation import BackupAutomation
from src.disaster_recovery.backup_manager import BackupManager, BackupStatus, BackupType
from src.disaster_recovery.dr_coordinator import (
    DisasterRecoveryCoordinator,
    DREventType,
    FailoverStatus,
    HealthStatus,
)
from src.disaster_recovery.runbooks.dr_runbook import (
    DisasterRecoveryRunbook,
    FailureType,
    RunbookStep,
)


@pytest.fixture(autouse=True)
def offline_aws(monkeypatch):
    """Keep boto3 from probing the EC2 metadata endpoint (slow timeout off AWS)."""
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")


class TestBackupManager:
    """Test backup manager functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.config = {
            "backup_dir": self.temp_dir,
            "aws": {"region": "us-west-2", "backup_region": "us-east-1"},
            "postgresql": {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "username": "test_user",
                "password": "test_pass",
            },
            "s3": {"bucket": "test-bucket", "backup_bucket": "test-backup-bucket"},
            "kafka": {
                "bootstrap_servers": ["localhost:9092"],
                "critical_topics": ["transactions", "user_events"],
            },
            "encryption": {"auto_generate_key": True},
            "access_control": {"enabled": False},
            "audit": {"log_file": f"{self.temp_dir}/audit.log"},
        }
        self.backup_manager = BackupManager(self.config)

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)

    @patch("src.disaster_recovery.backup_manager.boto3")
    def test_init_aws_clients(self, mock_boto3):
        """Test AWS client initialization."""
        # Mock boto3 clients
        mock_s3_client = Mock()
        mock_rds_client = Mock()
        mock_ec2_client = Mock()
        mock_boto3.client.side_effect = lambda service, **kwargs: {
            "s3": mock_s3_client,
            "rds": mock_rds_client,
            "ec2": mock_ec2_client,
        }[service]

        backup_manager = BackupManager(self.config)

        assert backup_manager.s3_client == mock_s3_client
        assert backup_manager.rds_client == mock_rds_client
        assert backup_manager.ec2_client == mock_ec2_client

    @pytest.mark.asyncio
    async def test_create_full_backup_success(self):
        """Test successful full backup creation."""
        # Mock the backup methods
        with patch.object(
            self.backup_manager, "_backup_postgresql"
        ) as mock_pg, patch.object(
            self.backup_manager, "_backup_s3_data_lake"
        ) as mock_s3, patch.object(
            self.backup_manager, "_backup_kafka_topics"
        ) as mock_kafka, patch.object(
            self.backup_manager, "_calculate_backup_checksum"
        ) as mock_checksum, patch.object(
            self.backup_manager, "_save_backup_metadata"
        ) as mock_save_meta:
            # Configure mocks
            mock_pg.return_value = {
                "status": BackupStatus.COMPLETED,
                "size_bytes": 1000000,
                "backup_file": "/path/to/db.sql.gz",
            }
            mock_s3.return_value = {
                "status": BackupStatus.COMPLETED,
                "size_bytes": 5000000,
                "objects_backed_up": 100,
            }
            mock_kafka.return_value = {
                "status": BackupStatus.COMPLETED,
                "size_bytes": 2000000,
                "topics_backed_up": 5,
            }
            mock_checksum.return_value = "abc123def456"

            # Execute backup
            result = await self.backup_manager.create_full_backup("test_backup")

            # Verify results
            assert result["status"] == BackupStatus.COMPLETED
            assert result["backup_id"] == "test_backup"
            assert result["total_size_bytes"] == 8000000
            assert result["checksum"] == "abc123def456"
            assert "postgresql" in result["components"]
            assert "s3_data_lake" in result["components"]
            assert "kafka" in result["components"]

    @pytest.mark.asyncio
    async def test_create_full_backup_partial_failure(self):
        """Test backup with some component failures."""
        with patch.object(
            self.backup_manager, "_backup_postgresql"
        ) as mock_pg, patch.object(
            self.backup_manager, "_backup_s3_data_lake"
        ) as mock_s3, patch.object(
            self.backup_manager, "_backup_kafka_topics"
        ) as mock_kafka, patch.object(
            self.backup_manager, "_calculate_backup_checksum"
        ) as mock_checksum, patch.object(
            self.backup_manager, "_save_backup_metadata"
        ) as mock_save_meta:
            # Configure mocks - S3 backup fails
            mock_pg.return_value = {
                "status": BackupStatus.COMPLETED,
                "size_bytes": 1000000,
            }
            mock_s3.side_effect = Exception("S3 connection failed")
            mock_kafka.return_value = {
                "status": BackupStatus.COMPLETED,
                "size_bytes": 2000000,
            }
            mock_checksum.return_value = "abc123def456"

            result = await self.backup_manager.create_full_backup("test_backup")

            # Should fail due to S3 component failure
            assert result["status"] == BackupStatus.FAILED
            assert result["components"]["s3_data_lake"]["status"] == BackupStatus.FAILED
            assert (
                "S3 connection failed" in result["components"]["s3_data_lake"]["error"]
            )

    @pytest.mark.asyncio
    @patch("src.disaster_recovery.backup_manager.asyncio.create_subprocess_exec")
    async def test_backup_postgresql(self, mock_subprocess):
        """Test PostgreSQL backup functionality."""
        # Mock subprocess
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.communicate = AsyncMock(return_value=(b"Backup completed", b""))
        mock_subprocess.return_value = mock_process

        # Mock file operations
        with patch(
            "src.disaster_recovery.backup_manager.Path.stat"
        ) as mock_stat, patch(
            "src.disaster_recovery.backup_manager.Path.unlink"
        ), patch.object(
            self.backup_manager, "_compress_file"
        ) as mock_compress, patch.object(
            self.backup_manager, "_encrypt_file"
        ), patch.object(
            self.backup_manager, "_verify_postgresql_backup"
        ) as mock_verify, patch.object(
            self.backup_manager, "_get_table_count"
        ) as mock_count:
            mock_stat.return_value.st_size = 1000000
            mock_count.return_value = 15

            backup_path = Path(self.temp_dir) / "test_backup"
            backup_path.mkdir(parents=True, exist_ok=True)

            result = await self.backup_manager._backup_postgresql(backup_path)

            assert result["status"] == BackupStatus.COMPLETED
            assert result["size_bytes"] == 1000000
            assert result["tables_backed_up"] == 15
            assert "postgresql" in result["backup_file"]

    @pytest.mark.asyncio
    async def test_list_backups(self):
        """Test backup listing functionality."""
        # Create test backup directories with metadata
        backup1_dir = Path(self.temp_dir) / "backup1"
        backup1_dir.mkdir(parents=True, exist_ok=True)

        metadata1 = {
            "backup_id": "backup1",
            "start_time": "2024-01-01T12:00:00",
            "status": BackupStatus.COMPLETED,
            "total_size_bytes": 1000000,
        }

        with open(backup1_dir / "backup_metadata.json", "w") as f:
            json.dump(metadata1, f)

        backups = await self.backup_manager.list_backups()

        assert len(backups) == 1
        assert backups[0]["backup_id"] == "backup1"
        assert backups[0]["location"] == "local"

    @pytest.mark.asyncio
    async def test_cleanup_failed_backups(self):
        """Test cleanup of failed backups."""
        # Create failed backup directory
        failed_backup_dir = Path(self.temp_dir) / "failed_backup"
        failed_backup_dir.mkdir(parents=True, exist_ok=True)

        metadata = {
            "backup_id": "failed_backup",
            "status": BackupStatus.FAILED,
            "error": "Test failure",
        }

        with open(failed_backup_dir / "backup_metadata.json", "w") as f:
            json.dump(metadata, f)

        # Cleanup failed backups
        cleaned_up = await self.backup_manager.cleanup_failed_backups()

        assert "failed_backup" in cleaned_up
        assert not failed_backup_dir.exists()

    def test_encryption_key_generation(self):
        """Test encryption key generation."""
        key = self.backup_manager._generate_encryption_key()

        assert len(key) == 32  # 256-bit key
        assert isinstance(key, bytes)

        # Test that key is stored in environment
        import os

        assert "ECAP_BACKUP_ENCRYPTION_KEY" in os.environ

    def test_access_control_disabled(self):
        """Test access control when disabled."""
        # Access control is disabled in test config
        result = asyncio.run(self.backup_manager._check_access_permissions("create"))
        assert result is True

    def test_access_control_enabled(self):
        """Test access control when enabled."""
        # Enable access control
        self.backup_manager.config["access_control"]["enabled"] = True
        self.backup_manager.config["access_control"]["users"] = {
            "test_user": ["backup.create", "backup.list"]
        }

        import os

        os.environ["USER"] = "test_user"

        # Should have create permission
        result = asyncio.run(self.backup_manager._check_access_permissions("create"))
        assert result is True

        # Should not have restore permission
        result = asyncio.run(self.backup_manager._check_access_permissions("restore"))
        assert result is False

    @pytest.mark.asyncio
    async def test_audit_logging(self):
        """Test audit logging functionality."""
        await self.backup_manager._log_audit_event("test_event", {"test": "data"})

        # Check audit file was created
        audit_file = Path(self.config["audit"]["log_file"])
        assert audit_file.exists()

        # Check audit entry
        with open(audit_file) as f:
            line = f.readline()
            audit_entry = json.loads(line)

        assert audit_entry["event_type"] == "test_event"
        assert audit_entry["details"]["test"] == "data"
        assert "timestamp" in audit_entry

    @pytest.mark.asyncio
    async def test_backup_integrity_verification(self):
        """Test backup integrity verification."""
        # Create a test backup directory with metadata
        backup_dir = Path(self.temp_dir) / "test_backup"
        backup_dir.mkdir(parents=True)

        metadata = {
            "backup_id": "test_backup",
            "start_time": "2024-01-01T12:00:00",
            "status": "completed",
            "checksum": "test_checksum",
        }

        with open(backup_dir / "backup_metadata.json", "w") as f:
            json.dump(metadata, f)

        # Mock _calculate_backup_checksum to return matching checksum
        with patch.object(
            self.backup_manager, "_calculate_backup_checksum"
        ) as mock_checksum:
            mock_checksum.return_value = "test_checksum"

            result = await self.backup_manager.verify_backup_integrity("test_backup")

        assert result["success"] is True
        assert result["verification_results"]["overall_status"] == "passed"
        assert result["verification_results"]["checksum_verification"]["valid"] is True

    @pytest.mark.asyncio
    async def test_compliance_report_generation(self):
        """Test compliance report generation."""
        # Create test backups
        backup_dir = Path(self.temp_dir) / "test_backup"
        backup_dir.mkdir(parents=True)

        metadata = {
            "backup_id": "test_backup",
            "start_time": datetime.now().isoformat(),
            "status": "completed",
            "components": {"postgresql": {"encryption": "AES-256-GCM"}},
        }

        with open(backup_dir / "backup_metadata.json", "w") as f:
            json.dump(metadata, f)

        result = await self.backup_manager.generate_compliance_report()

        assert result["success"] is True
        assert "report" in result
        assert result["report"]["summary"]["total_backups"] >= 1
        assert "security_compliance" in result["report"]


class TestDisasterRecoveryCoordinator:
    """Test disaster recovery coordinator functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.config = {
            "primary_region": "us-west-2",
            "backup_region": "us-east-1",
            "rto_target_minutes": 15,
            "rpo_target_minutes": 5,
            "monitored_services": {
                "api_service": {
                    "type": "http",
                    "health_url": "http://localhost:8000/health",
                    "expected_status": 200,
                },
                "database": {
                    "type": "database",
                    "db_type": "postgresql",
                    "host": "localhost",
                    "port": 5432,
                    "database": "test_db",
                    "user": "test_user",
                    "password": "test_pass",
                },
            },
            "critical_services": ["api_service", "database"],
            "failover_threshold": 0.5,
            "auto_failover_enabled": True,
        }
        self.dr_coordinator = DisasterRecoveryCoordinator(self.config)

    @pytest.mark.asyncio
    async def test_check_http_health_success(self):
        """Test HTTP health check success."""
        with patch("aiohttp.ClientSession.get") as mock_get:
            # Mock successful HTTP response
            mock_response = Mock()
            mock_response.status = 200
            mock_get.return_value.__aenter__.return_value = mock_response

            result = await self.dr_coordinator._check_http_health(
                "api_service",
                {"health_url": "http://localhost:8000/health", "expected_status": 200},
            )

            assert result["status"] == HealthStatus.HEALTHY
            assert result["status_code"] == 200
            assert "response_time_ms" in result

    @pytest.mark.asyncio
    async def test_check_http_health_failure(self):
        """Test HTTP health check failure."""
        with patch("aiohttp.ClientSession.get") as mock_get:
            # Mock failed HTTP response
            mock_response = Mock()
            mock_response.status = 500
            mock_get.return_value.__aenter__.return_value = mock_response

            result = await self.dr_coordinator._check_http_health(
                "api_service",
                {"health_url": "http://localhost:8000/health", "expected_status": 200},
            )

            assert result["status"] == HealthStatus.UNHEALTHY
            assert result["status_code"] == 500
            assert "Unexpected status code" in result["error"]

    @pytest.mark.asyncio
    async def test_check_postgresql_health(self):
        """Test PostgreSQL health check."""
        with patch("psycopg2.connect") as mock_connect:
            # Mock successful database connection
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_connect.return_value = mock_conn

            config = {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "user": "test_user",
                "password": "test_pass",
            }

            result = await self.dr_coordinator._check_postgresql_health(
                "database", config
            )

            assert result["status"] == HealthStatus.HEALTHY
            assert "response_time_ms" in result

    @pytest.mark.asyncio
    async def test_evaluate_failover_conditions_trigger(self):
        """Test failover trigger conditions."""
        # Set service health to unhealthy for critical services
        self.dr_coordinator.service_health = {
            "api_service": {"status": HealthStatus.UNHEALTHY},
            "database": {"status": HealthStatus.UNHEALTHY},
        }

        with patch.object(self.dr_coordinator, "_initiate_failover") as mock_failover:
            await self.dr_coordinator._evaluate_failover_conditions()
            mock_failover.assert_called_once()

    @pytest.mark.asyncio
    async def test_evaluate_failover_conditions_no_trigger(self):
        """Test failover not triggered when services are healthy."""
        # Set service health to healthy
        self.dr_coordinator.service_health = {
            "api_service": {"status": HealthStatus.HEALTHY},
            "database": {"status": HealthStatus.HEALTHY},
        }

        with patch.object(self.dr_coordinator, "_initiate_failover") as mock_failover:
            await self.dr_coordinator._evaluate_failover_conditions()
            mock_failover.assert_not_called()

    @pytest.mark.asyncio
    async def test_initiate_failover(self):
        """Test failover initiation."""
        with patch.object(
            self.dr_coordinator, "_perform_dns_failover"
        ) as mock_dns, patch.object(
            self.dr_coordinator, "_perform_database_failover"
        ) as mock_db, patch.object(
            self.dr_coordinator, "_perform_storage_failover"
        ) as mock_storage, patch.object(
            self.dr_coordinator, "_perform_compute_failover"
        ) as mock_compute, patch.object(
            self.dr_coordinator, "_verify_failover"
        ) as mock_verify:
            await self.dr_coordinator._initiate_failover("Test failover")

            # Verify all failover steps were called
            mock_dns.assert_called_once()
            mock_db.assert_called_once()
            mock_storage.assert_called_once()
            mock_compute.assert_called_once()
            mock_verify.assert_called_once()

            # Verify status changes
            assert self.dr_coordinator.failover_status == FailoverStatus.ACTIVE
            assert self.dr_coordinator.current_region == "us-east-1"
            assert self.dr_coordinator.backup_region == "us-west-2"

    @pytest.mark.asyncio
    async def test_get_rto_rpo_metrics(self):
        """Test RTO/RPO metrics calculation."""
        # Add a completed failover event
        self.dr_coordinator.event_history.append(
            {
                "event_type": DREventType.FAILOVER_COMPLETED,
                "timestamp": datetime.now().isoformat(),
                "data": {"failover_duration_seconds": 600},  # 10 minutes
            }
        )

        metrics = await self.dr_coordinator.get_rto_rpo_metrics()

        assert metrics["rto_target_minutes"] == 15
        assert metrics["rpo_target_minutes"] == 5
        assert metrics["last_rto_minutes"] == 10
        assert metrics["rto_compliance"] is True  # 10 minutes < 15 minute target

    @pytest.mark.asyncio
    async def test_manual_failover(self):
        """Test manual failover trigger."""
        with patch.object(self.dr_coordinator, "_initiate_failover") as mock_failover:
            result = await self.dr_coordinator.manual_failover("Manual test")

            assert result["success"] is True
            mock_failover.assert_called_once_with("Manual failover: Manual test")

    @pytest.mark.asyncio
    async def test_get_health_status(self):
        """Test health status reporting."""
        self.dr_coordinator.service_health = {
            "api_service": {"status": HealthStatus.HEALTHY},
            "database": {"status": HealthStatus.DEGRADED},
        }

        status = await self.dr_coordinator.get_health_status()

        assert status["overall_status"] == HealthStatus.DEGRADED.value
        assert status["current_region"] == "us-west-2"
        assert status["backup_region"] == "us-east-1"
        assert len(status["services"]) == 2


class TestBackupAutomation:
    """Test backup automation functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.config = {
            "backup_manager": {"backup_dir": self.temp_dir},
            "backup_schedules": {
                "daily_full": {"time": "02:00"},
                "hourly_incremental": {"interval": 1},
            },
            "monitoring": {"enable_metrics": False},  # Disable for testing
            "alerting": {
                "rules": {"max_backup_duration_minutes": 60, "max_backup_size_gb": 100},
                "notifications": {
                    "email": {"enabled": False},
                    "slack": {"enabled": False},
                    "webhook": {"enabled": False},
                },
            },
        }
        self.backup_automation = BackupAutomation(self.config)

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)

    @pytest.mark.asyncio
    async def test_execute_scheduled_backup_success(self):
        """Test successful scheduled backup execution."""
        with patch.object(
            self.backup_automation.backup_manager, "create_full_backup"
        ) as mock_backup:
            mock_backup.return_value = {
                "backup_id": "test_backup",
                "status": BackupStatus.COMPLETED,
                "total_size_bytes": 1000000,
                "duration_seconds": 300,
            }

            await self.backup_automation._execute_scheduled_backup(
                BackupType.FULL, "daily_full"
            )

            mock_backup.assert_called_once()
            assert (
                BackupType.FULL.value in self.backup_automation.last_successful_backup
            )

    @pytest.mark.asyncio
    async def test_execute_scheduled_backup_failure(self):
        """Test scheduled backup failure handling."""
        with patch.object(
            self.backup_automation.backup_manager, "create_full_backup"
        ) as mock_backup, patch.object(
            self.backup_automation, "_send_notification"
        ) as mock_notify:
            mock_backup.return_value = {
                "backup_id": "test_backup",
                "status": BackupStatus.FAILED,
                "error": "Test error",
            }

            await self.backup_automation._execute_scheduled_backup(
                BackupType.FULL, "daily_full"
            )

            # The backup name embeds the current timestamp, so match on its stable parts
            # The last notification is the failure notice (an alert is sent first)
            title, message = mock_notify.call_args.args
            assert title == "Backup Failed"
            assert message.startswith("Backup daily_full_")
            assert message.endswith(" failed: Test error")
            assert mock_notify.call_args.kwargs == {"severity": "error"}

    def test_update_backup_metrics(self):
        """Test backup metrics update."""
        result = {
            "backup_id": "test_backup",
            "status": BackupStatus.COMPLETED,
            "total_size_bytes": 1000000,
        }

        self.backup_automation._update_backup_metrics(BackupType.FULL, result, 300.0)

        assert BackupType.FULL.value in self.backup_automation.backup_metrics
        metrics = self.backup_automation.backup_metrics[BackupType.FULL.value]
        assert metrics["last_status"] == BackupStatus.COMPLETED.value
        assert metrics["last_duration_seconds"] == 300.0
        assert metrics["last_size_bytes"] == 1000000

    @pytest.mark.asyncio
    async def test_check_backup_alerts_duration(self):
        """Test backup duration alert triggering."""
        with patch.object(self.backup_automation, "_trigger_alert") as mock_alert:
            result = {
                "status": BackupStatus.COMPLETED,
                "duration_seconds": 4000,  # 66 minutes > 60 minute threshold
            }

            await self.backup_automation._check_backup_alerts(result, "test_backup")

            mock_alert.assert_called_with(
                "backup_duration_exceeded",
                "Backup duration exceeded threshold: test_backup",
                "Backup took 66.7 minutes (threshold: 60 minutes)",
                severity="warning",
            )

    @pytest.mark.asyncio
    async def test_check_stale_backups(self):
        """Test stale backup detection."""
        # Set last successful backup to 26 hours ago
        self.backup_automation.last_successful_backup[
            "daily"
        ] = datetime.now() - timedelta(hours=26)

        with patch.object(self.backup_automation, "_trigger_alert") as mock_alert:
            await self.backup_automation._check_stale_backups()

            mock_alert.assert_called_with(
                "backup_stale",
                "Backup is stale: daily",
                "Last successful backup was 26.0 hours ago (threshold: 25 hours)",
                severity="warning",
            )

    @pytest.mark.asyncio
    async def test_trigger_manual_backup(self):
        """Test manual backup triggering."""
        with patch.object(
            self.backup_automation.backup_manager, "create_full_backup"
        ) as mock_backup:
            mock_backup.return_value = {
                "backup_id": "manual_backup",
                "status": BackupStatus.COMPLETED,
            }

            result = await self.backup_automation.trigger_manual_backup("full")

            assert result["success"] is True
            assert result["backup_id"] == "manual_backup"
            mock_backup.assert_called_once()


class TestDisasterRecoveryRunbook:
    """Test disaster recovery runbook functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.runbook = DisasterRecoveryRunbook()

    def test_get_runbook(self):
        """Test getting runbook for failure type."""
        db_runbook = self.runbook.get_runbook(FailureType.DATABASE_FAILURE)
        assert len(db_runbook) > 0
        assert all(isinstance(step, RunbookStep) for step in db_runbook)

    def test_runbook_step_creation(self):
        """Test runbook step creation and serialization."""
        step = RunbookStep(
            step_id="test_01",
            title="Test Step",
            description="Test step description",
            estimated_time_minutes=5,
            commands=["echo 'test'"],
            verification="Test verification",
            dependencies=["prereq_01"],
        )

        step_dict = step.to_dict()

        assert step_dict["step_id"] == "test_01"
        assert step_dict["title"] == "Test Step"
        assert step_dict["commands"] == ["echo 'test'"]
        assert step_dict["status"] == "pending"

    def test_execute_runbook_manual(self):
        """Test manual runbook execution."""
        # Create a simple test runbook
        test_runbook = [
            RunbookStep(
                step_id="test_01",
                title="Test Step 1",
                description="First test step",
                estimated_time_minutes=1,
            ),
            RunbookStep(
                step_id="test_02",
                title="Test Step 2",
                description="Second test step",
                estimated_time_minutes=1,
                dependencies=["test_01"],
            ),
        ]

        # Mock the runbook
        with patch.object(self.runbook, "get_runbook") as mock_get_runbook:
            mock_get_runbook.return_value = test_runbook

            result = self.runbook.execute_runbook(
                FailureType.DATABASE_FAILURE,
                auto_execute=False,
                skip_confirmations=True,
            )

            assert result["success"] is True
            assert result["execution_log"]["overall_status"] == "completed"
            assert len(result["execution_log"]["steps"]) == 2

    def test_execute_runbook_with_dependencies(self):
        """Test runbook execution with step dependencies."""
        # Create steps with dependencies
        step1 = RunbookStep(
            step_id="step_01",
            title="Step 1",
            description="First step",
            estimated_time_minutes=1,
        )

        step2 = RunbookStep(
            step_id="step_02",
            title="Step 2",
            description="Second step",
            estimated_time_minutes=1,
            dependencies=["step_01"],
        )

        # Simulate step 1 failure
        with patch.object(
            self.runbook, "get_runbook"
        ) as mock_get_runbook, patch.object(
            self.runbook, "_execute_step"
        ) as mock_execute_step:
            mock_get_runbook.return_value = [step1, step2]

            # First step fails, second should be skipped
            def side_effect(step, auto_execute, skip_confirmations):
                if step.step_id == "step_01":
                    step.status = "failed"
                    step.error = "Simulated failure"
                else:
                    step.status = "skipped"
                    step.error = "Dependencies not met"
                return step.to_dict()

            mock_execute_step.side_effect = side_effect

            result = self.runbook.execute_runbook(
                FailureType.DATABASE_FAILURE, skip_confirmations=True
            )

            assert result["success"] is False
            assert result["execution_log"]["overall_status"] == "failed"

    def test_generate_runbook_documentation(self):
        """Test runbook documentation generation."""
        doc = self.runbook.generate_runbook_documentation(FailureType.DATABASE_FAILURE)

        assert "Database Failure" in doc
        assert "## Steps" in doc
        assert "## Prerequisites" in doc
        assert "## Post-Recovery Actions" in doc

    def test_get_all_runbooks_summary(self):
        """Test getting summary of all runbooks."""
        summary = self.runbook.get_all_runbooks_summary()

        assert len(summary) > 0
        assert "database_failure" in summary
        assert "total_steps" in summary["database_failure"]
        assert "estimated_time_minutes" in summary["database_failure"]


class TestRunbookStep:
    """Test runbook step functionality."""

    def test_step_initialization(self):
        """Test step initialization with all parameters."""
        step = RunbookStep(
            step_id="test_01",
            title="Test Step",
            description="Test description",
            estimated_time_minutes=10,
            commands=["echo 'hello'", "ls -la"],
            verification="Check output",
            rollback="Undo changes",
            dependencies=["prereq_01", "prereq_02"],
        )

        assert step.step_id == "test_01"
        assert step.title == "Test Step"
        assert step.description == "Test description"
        assert step.estimated_time_minutes == 10
        assert len(step.commands) == 2
        assert step.verification == "Check output"
        assert step.rollback == "Undo changes"
        assert len(step.dependencies) == 2
        assert step.status == "pending"

    def test_step_to_dict(self):
        """Test step serialization to dictionary."""
        step = RunbookStep(
            step_id="test_01",
            title="Test Step",
            description="Test description",
            estimated_time_minutes=5,
        )

        step.status = "completed"
        step.start_time = datetime.now()
        step.end_time = datetime.now()
        step.output = "Test output"

        step_dict = step.to_dict()

        assert step_dict["step_id"] == "test_01"
        assert step_dict["status"] == "completed"
        assert step_dict["start_time"] is not None
        assert step_dict["end_time"] is not None
        assert step_dict["output"] == "Test output"


if __name__ == "__main__":
    pytest.main([__file__])
