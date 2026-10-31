"""
Backup Automation Service for E-Commerce Analytics Platform

This module provides automated backup scheduling, monitoring,
and alerting capabilities for the disaster recovery system.
"""

import asyncio
import json
import logging
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional, Tuple

import schedule
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from .backup_manager import BackupManager, BackupStatus, BackupType

logger = logging.getLogger(__name__)


# Prometheus metrics
backup_total = Counter("backups_total", "Total number of backups", ["type", "status"])
backup_duration = Histogram(
    "backup_duration_seconds", "Backup duration in seconds", ["type"]
)
backup_size = Gauge("backup_size_bytes", "Backup size in bytes", ["backup_id"])
backup_age = Gauge(
    "backup_age_hours", "Age of last successful backup in hours", ["type"]
)


class BackupAutomation:
    """
    Automated backup scheduling and monitoring service.

    Features:
    - Scheduled backup execution
    - Backup monitoring and alerting
    - Retention policy enforcement
    - Performance metrics collection
    - Health checks and reporting
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the backup automation service.

        Args:
            config: Configuration dictionary containing automation settings
        """
        self.config = config
        self.backup_manager = BackupManager(config.get("backup_manager", {}))

        # Scheduling configuration
        self.backup_schedules = config.get("backup_schedules", {})

        # Monitoring configuration
        self.monitoring_config = config.get("monitoring", {})
        self.alert_config = config.get("alerting", {})

        # Performance tracking
        self.backup_metrics = {}
        self.last_successful_backup = {}

        # Alert state tracking
        self.alert_states = {}

        # Prometheus metrics server
        self.metrics_port = config.get("metrics_port", 8000)

        # Running state
        self.is_running = False
        self.scheduler_task = None

    async def start(self):
        """Start the backup automation service."""
        if self.is_running:
            logger.warning("Backup automation service is already running")
            return

        logger.info("Starting backup automation service")
        self.is_running = True

        # Start Prometheus metrics server
        if self.monitoring_config.get("enable_metrics", True):
            start_http_server(self.metrics_port)
            logger.info(f"Metrics server started on port {self.metrics_port}")

        # Configure backup schedules
        await self._configure_schedules()

        # Start scheduler
        self.scheduler_task = asyncio.create_task(self._run_scheduler())

        # Start monitoring
        monitoring_task = asyncio.create_task(self._run_monitoring())

        # Wait for tasks to complete
        await asyncio.gather(self.scheduler_task, monitoring_task)

    async def stop(self):
        """Stop the backup automation service."""
        logger.info("Stopping backup automation service")
        self.is_running = False

        if self.scheduler_task:
            self.scheduler_task.cancel()

    async def _configure_schedules(self):
        """Configure backup schedules."""
        logger.info("Configuring backup schedules")

        # Clear existing schedules
        schedule.clear()

        # Configure scheduled backups
        schedules = self.backup_schedules

        # Daily full backups
        daily_schedule = schedules.get("daily_full")
        if daily_schedule:
            time_str = daily_schedule.get("time", "02:00")
            schedule.every().day.at(time_str).do(
                self._schedule_backup,
                backup_type=BackupType.FULL,
                schedule_name="daily_full",
            )
            logger.info(f"Scheduled daily full backup at {time_str}")

        # Hourly incremental backups
        incremental_schedule = schedules.get("hourly_incremental")
        if incremental_schedule:
            interval = incremental_schedule.get("interval", 1)
            schedule.every(interval).hours.do(
                self._schedule_backup,
                backup_type=BackupType.INCREMENTAL,
                schedule_name="hourly_incremental",
            )
            logger.info(f"Scheduled incremental backup every {interval} hour(s)")

        # Weekly full backups
        weekly_schedule = schedules.get("weekly_full")
        if weekly_schedule:
            day = weekly_schedule.get("day", "sunday")
            time_str = weekly_schedule.get("time", "01:00")
            getattr(schedule.every(), day.lower()).at(time_str).do(
                self._schedule_backup,
                backup_type=BackupType.FULL,
                schedule_name="weekly_full",
            )
            logger.info(f"Scheduled weekly full backup on {day} at {time_str}")

        # Monthly full backups
        monthly_schedule = schedules.get("monthly_full")
        if monthly_schedule:
            day = monthly_schedule.get("day", 1)
            time_str = monthly_schedule.get("time", "00:00")
            # Note: schedule library doesn't have monthly, so we'll check manually
            logger.info(f"Configured monthly full backup on day {day} at {time_str}")

    def _schedule_backup(self, backup_type: BackupType, schedule_name: str):
        """Schedule a backup job."""
        logger.info(f"Scheduling {backup_type.value} backup: {schedule_name}")

        # Create async task for backup
        asyncio.create_task(self._execute_scheduled_backup(backup_type, schedule_name))

    async def _execute_scheduled_backup(
        self, backup_type: BackupType, schedule_name: str
    ):
        """Execute a scheduled backup."""
        backup_name = f"{schedule_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        logger.info(f"Executing scheduled backup: {backup_name}")

        start_time = datetime.now()

        try:
            # Execute backup
            if backup_type == BackupType.FULL:
                result = await self.backup_manager.create_full_backup(backup_name)
            elif backup_type == BackupType.INCREMENTAL:
                result = await self._create_incremental_backup(backup_name)
            else:
                result = {
                    "status": BackupStatus.FAILED,
                    "error": "Unsupported backup type",
                }

            # Calculate duration
            duration = (datetime.now() - start_time).total_seconds()

            # Update metrics
            self._update_backup_metrics(backup_type, result, duration)

            # Check for alerts
            await self._check_backup_alerts(result, schedule_name)

            # Send notifications
            if result["status"] == BackupStatus.COMPLETED:
                logger.info(f"Scheduled backup completed successfully: {backup_name}")
                self.last_successful_backup[backup_type.value] = datetime.now()

                if self.alert_config.get("success_notifications", False):
                    await self._send_notification(
                        "Backup Successful",
                        f"Backup {backup_name} completed successfully in {duration:.1f} seconds",
                        severity="info",
                    )
            else:
                logger.error(
                    f"Scheduled backup failed: {backup_name} - {result.get('error')}"
                )
                await self._send_notification(
                    "Backup Failed",
                    f"Backup {backup_name} failed: {result.get('error')}",
                    severity="error",
                )

        except Exception as e:
            logger.error(f"Scheduled backup execution failed: {e}")
            await self._send_notification(
                "Backup Execution Error",
                f"Failed to execute backup {backup_name}: {str(e)}",
                severity="error",
            )

    async def _create_incremental_backup(self, backup_name: str) -> Dict[str, Any]:
        """Create an incremental backup (simplified implementation)."""
        # For now, this is a simplified incremental backup
        # In a real implementation, this would only backup changed data

        logger.info(f"Creating incremental backup: {backup_name}")

        # For demonstration, we'll create a smaller backup focusing on recent data
        try:
            # Backup only critical components with recent data
            result = await self.backup_manager.create_full_backup(
                backup_name=backup_name,
                include_data_lake=False,  # Skip large data lake for incremental
                include_database=True,  # Include database changes
                include_kafka=True,  # Include recent Kafka messages
            )

            # Mark as incremental in metadata
            if "backup_type" in result:
                result["backup_type"] = BackupType.INCREMENTAL

            return result

        except Exception as e:
            return {
                "status": BackupStatus.FAILED,
                "error": str(e),
                "backup_type": BackupType.INCREMENTAL,
            }

    def _update_backup_metrics(
        self, backup_type: BackupType, result: Dict[str, Any], duration: float
    ):
        """Update Prometheus metrics for backup."""
        status = result.get("status", BackupStatus.FAILED)

        # Update counters
        backup_total.labels(type=backup_type.value, status=status.value).inc()

        # Update duration
        backup_duration.labels(type=backup_type.value).observe(duration)

        # Update size if available
        total_size = result.get("total_size_bytes", 0)
        if total_size > 0:
            backup_id = result.get("backup_id", "unknown")
            backup_size.labels(backup_id=backup_id).set(total_size)

        # Update age metric for successful backups
        if status == BackupStatus.COMPLETED:
            backup_age.labels(type=backup_type.value).set(0)  # Reset age to 0

        # Store metrics locally
        self.backup_metrics[backup_type.value] = {
            "last_execution": datetime.now().isoformat(),
            "last_status": status.value,
            "last_duration_seconds": duration,
            "last_size_bytes": total_size,
        }

    async def _check_backup_alerts(self, result: Dict[str, Any], schedule_name: str):
        """Check if backup results should trigger alerts."""
        alert_rules = self.alert_config.get("rules", {})

        # Check for backup failure
        if result.get("status") == BackupStatus.FAILED:
            await self._trigger_alert(
                "backup_failed",
                f"Backup failed: {schedule_name}",
                result.get("error", "Unknown error"),
                severity="critical",
            )

        # Check backup duration
        duration = result.get("duration_seconds", 0)
        max_duration = alert_rules.get("max_backup_duration_minutes", 60) * 60

        if duration > max_duration:
            await self._trigger_alert(
                "backup_duration_exceeded",
                f"Backup duration exceeded threshold: {schedule_name}",
                f"Backup took {duration/60:.1f} minutes (threshold: {max_duration/60:g} minutes)",
                severity="warning",
            )

        # Check backup size
        total_size = result.get("total_size_bytes", 0)
        if total_size > 0:
            max_size_gb = alert_rules.get("max_backup_size_gb", 100)
            max_size_bytes = max_size_gb * 1024 * 1024 * 1024

            if total_size > max_size_bytes:
                await self._trigger_alert(
                    "backup_size_exceeded",
                    f"Backup size exceeded threshold: {schedule_name}",
                    f"Backup size: {total_size/1024/1024/1024:.1f} GB (threshold: {max_size_gb} GB)",
                    severity="warning",
                )

        # Check component failures
        components = result.get("components", {})
        failed_components = [
            comp
            for comp, details in components.items()
            if details.get("status") == BackupStatus.FAILED
        ]

        if failed_components:
            await self._trigger_alert(
                "backup_component_failed",
                f"Backup components failed: {schedule_name}",
                f"Failed components: {', '.join(failed_components)}",
                severity="warning",
            )

    async def _trigger_alert(
        self, alert_type: str, title: str, message: str, severity: str
    ):
        """Trigger an alert."""
        alert_key = f"{alert_type}_{title}"

        # Check if alert is already active (avoid spam)
        if alert_key in self.alert_states:
            last_alert_time = self.alert_states[alert_key]
            min_interval = timedelta(
                minutes=self.alert_config.get("min_alert_interval_minutes", 30)
            )

            if datetime.now() - last_alert_time < min_interval:
                logger.debug(f"Skipping duplicate alert: {alert_key}")
                return

        # Record alert
        self.alert_states[alert_key] = datetime.now()

        logger.warning(f"Alert triggered: {title} - {message}")

        # Send notification
        await self._send_notification(title, message, severity)

    async def _send_notification(self, title: str, message: str, severity: str):
        """Send notification via configured channels."""
        notification_config = self.alert_config.get("notifications", {})

        # Email notifications
        if notification_config.get("email", {}).get("enabled", False):
            await self._send_email_notification(title, message, severity)

        # Slack notifications
        if notification_config.get("slack", {}).get("enabled", False):
            await self._send_slack_notification(title, message, severity)

        # Webhook notifications
        if notification_config.get("webhook", {}).get("enabled", False):
            await self._send_webhook_notification(title, message, severity)

    async def _send_email_notification(self, title: str, message: str, severity: str):
        """Send email notification."""
        try:
            email_config = self.alert_config["notifications"]["email"]

            # Create message
            msg = MIMEMultipart()
            msg["From"] = email_config["from_address"]
            msg["To"] = ", ".join(email_config["to_addresses"])
            msg["Subject"] = f"[{severity.upper()}] ECAP Backup Alert: {title}"

            # Email body
            body = f"""
            E-Commerce Analytics Platform Backup Alert

            Alert: {title}
            Severity: {severity.upper()}
            Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

            Details:
            {message}

            This is an automated alert from the ECAP disaster recovery system.
            """

            msg.attach(MIMEText(body, "plain"))

            # Send email
            server = smtplib.SMTP(
                email_config["smtp_server"], email_config["smtp_port"]
            )

            if email_config.get("use_tls", True):
                server.starttls()

            if email_config.get("username") and email_config.get("password"):
                server.login(email_config["username"], email_config["password"])

            server.send_message(msg)
            server.quit()

            logger.info(f"Email notification sent: {title}")

        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")

    async def _send_slack_notification(self, title: str, message: str, severity: str):
        """Send Slack notification."""
        try:
            import aiohttp

            slack_config = self.alert_config["notifications"]["slack"]
            webhook_url = slack_config["webhook_url"]

            # Determine color based on severity
            color_map = {
                "info": "#36a64f",  # Green
                "warning": "#ff9500",  # Orange
                "error": "#ff0000",  # Red
                "critical": "#800000",  # Dark red
            }
            color = color_map.get(severity, "#808080")

            # Create Slack message
            slack_message = {
                "attachments": [
                    {
                        "color": color,
                        "title": f"ECAP Backup Alert: {title}",
                        "text": message,
                        "fields": [
                            {
                                "title": "Severity",
                                "value": severity.upper(),
                                "short": True,
                            },
                            {
                                "title": "Time",
                                "value": datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S UTC"
                                ),
                                "short": True,
                            },
                        ],
                        "footer": "ECAP Disaster Recovery System",
                        "ts": int(datetime.now().timestamp()),
                    }
                ]
            }

            # Send to Slack
            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json=slack_message) as response:
                    if response.status == 200:
                        logger.info(f"Slack notification sent: {title}")
                    else:
                        logger.error(
                            f"Failed to send Slack notification: HTTP {response.status}"
                        )

        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")

    async def _send_webhook_notification(self, title: str, message: str, severity: str):
        """Send webhook notification."""
        try:
            import aiohttp

            webhook_config = self.alert_config["notifications"]["webhook"]
            webhook_url = webhook_config["url"]

            # Create webhook payload
            payload = {
                "title": title,
                "message": message,
                "severity": severity,
                "timestamp": datetime.now().isoformat(),
                "source": "ecap_backup_automation",
                "environment": self.config.get("environment", "unknown"),
            }

            # Add custom headers if configured
            headers = webhook_config.get("headers", {})

            # Send webhook
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    webhook_url, json=payload, headers=headers
                ) as response:
                    if 200 <= response.status < 300:
                        logger.info(f"Webhook notification sent: {title}")
                    else:
                        logger.error(
                            f"Failed to send webhook notification: HTTP {response.status}"
                        )

        except Exception as e:
            logger.error(f"Failed to send webhook notification: {e}")

    async def _run_scheduler(self):
        """Run the backup scheduler."""
        logger.info("Starting backup scheduler")

        while self.is_running:
            try:
                # Run pending scheduled jobs
                schedule.run_pending()

                # Check for monthly backups manually
                await self._check_monthly_backups()

                # Wait before next check
                await asyncio.sleep(60)  # Check every minute

            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                await asyncio.sleep(60)

    async def _check_monthly_backups(self):
        """Check if monthly backup should be executed."""
        monthly_schedule = self.backup_schedules.get("monthly_full")
        if not monthly_schedule:
            return

        now = datetime.now()
        target_day = monthly_schedule.get("day", 1)
        target_time = monthly_schedule.get("time", "00:00")

        # Check if it's the right day and we haven't done a monthly backup today
        if now.day == target_day:
            time_parts = target_time.split(":")
            target_hour = int(time_parts[0])
            target_minute = int(time_parts[1]) if len(time_parts) > 1 else 0

            # Check if it's time for monthly backup
            if now.hour == target_hour and now.minute >= target_minute:
                # Check if we've already done a monthly backup today
                last_monthly = self.last_successful_backup.get("monthly_full")
                if not last_monthly or last_monthly.date() != now.date():
                    logger.info("Executing monthly backup")
                    await self._execute_scheduled_backup(
                        BackupType.FULL, "monthly_full"
                    )
                    self.last_successful_backup["monthly_full"] = now

    async def _run_monitoring(self):
        """Run backup monitoring and health checks."""
        logger.info("Starting backup monitoring")

        while self.is_running:
            try:
                # Update age metrics
                await self._update_age_metrics()

                # Check for stale backups
                await self._check_stale_backups()

                # Check disk space
                await self._check_disk_space()

                # Cleanup old backups
                await self._cleanup_old_backups()

                # Wait before next check
                await asyncio.sleep(300)  # Check every 5 minutes

            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(300)

    async def _update_age_metrics(self):
        """Update backup age metrics."""
        for backup_type, last_time in self.last_successful_backup.items():
            if last_time:
                age_hours = (datetime.now() - last_time).total_seconds() / 3600
                backup_age.labels(type=backup_type).set(age_hours)

    async def _check_stale_backups(self):
        """Check for stale backups and alert."""
        alert_rules = self.alert_config.get("rules", {})
        max_age_hours = alert_rules.get(
            "max_backup_age_hours", 25
        )  # 25 hours for daily backups

        for backup_type, last_time in self.last_successful_backup.items():
            if last_time:
                age_hours = (datetime.now() - last_time).total_seconds() / 3600

                if age_hours > max_age_hours:
                    await self._trigger_alert(
                        "backup_stale",
                        f"Backup is stale: {backup_type}",
                        f"Last successful backup was {age_hours:.1f} hours ago (threshold: {max_age_hours} hours)",
                        severity="warning",
                    )

    async def _check_disk_space(self):
        """Check backup disk space and alert if low."""
        try:
            import shutil

            backup_dir = self.backup_manager.backup_dir
            total, used, free = shutil.disk_usage(backup_dir)

            free_percent = (free / total) * 100
            min_free_percent = self.alert_config.get("rules", {}).get(
                "min_free_disk_percent", 20
            )

            if free_percent < min_free_percent:
                await self._trigger_alert(
                    "disk_space_low",
                    "Backup disk space is low",
                    f"Free space: {free_percent:.1f}% (threshold: {min_free_percent}%)",
                    severity="warning",
                )

        except Exception as e:
            logger.error(f"Failed to check disk space: {e}")

    async def _cleanup_old_backups(self):
        """Clean up old backups according to retention policies."""
        try:
            await self.backup_manager._apply_retention_policies()
        except Exception as e:
            logger.error(f"Failed to cleanup old backups: {e}")

    async def get_backup_status(self) -> Dict[str, Any]:
        """Get current backup automation status."""
        return {
            "service_status": "running" if self.is_running else "stopped",
            "scheduled_backups": self.backup_schedules,
            "last_successful_backups": {
                backup_type: last_time.isoformat() if last_time else None
                for backup_type, last_time in self.last_successful_backup.items()
            },
            "backup_metrics": self.backup_metrics,
            "alert_states": len(self.alert_states),
            "next_scheduled": self._get_next_scheduled_backup(),
        }

    def _get_next_scheduled_backup(self) -> Optional[str]:
        """Get the next scheduled backup time."""
        try:
            jobs = schedule.jobs
            if jobs:
                next_job = min(jobs, key=lambda job: job.next_run)
                return next_job.next_run.isoformat()
        except Exception:
            pass
        return None

    async def trigger_manual_backup(self, backup_type: str = "full") -> Dict[str, Any]:
        """Trigger a manual backup."""
        try:
            backup_name = (
                f"manual_{backup_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )

            if backup_type == "full":
                result = await self.backup_manager.create_full_backup(backup_name)
            elif backup_type == "incremental":
                result = await self._create_incremental_backup(backup_name)
            else:
                return {
                    "success": False,
                    "error": f"Unsupported backup type: {backup_type}",
                }

            return {
                "success": True,
                "backup_id": result.get("backup_id"),
                "status": result.get("status"),
            }

        except Exception as e:
            return {"success": False, "error": str(e)}
