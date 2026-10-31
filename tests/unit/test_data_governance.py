"""
Unit tests for data governance components.
"""

import shutil
import tempfile
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest

from src.data_governance.audit import AccessAuditor, AccessEventType
from src.data_governance.catalog import DataCatalog
from src.data_governance.lineage import DataLineageTracker, LineageEventType
from src.data_governance.privacy import ConsentType, DataSubjectRight, PrivacyManager
from src.data_governance.quality import (
    AlertSeverity,
    DataQualityMonitor,
    QualityCheckType,
)


class TestDataCatalog:
    """Test data catalog functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.catalog = DataCatalog(catalog_path=self.temp_dir)

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)

    def test_register_asset(self):
        """Test asset registration."""
        asset = self.catalog.register_asset(
            name="test_table",
            path="/data/test_table",
            format="parquet",
            description="Test table for unit tests",
            owner="test_user",
            classification="internal",
            tags=["test", "analytics"],
            auto_discover_schema=False,
        )

        assert asset.name == "test_table"
        assert asset.path == "/data/test_table"
        assert asset.format == "parquet"
        assert asset.classification == "internal"
        assert "test" in asset.tags
        assert "analytics" in asset.tags

        # Check if asset is stored
        assert "test_table" in self.catalog.assets

    def test_get_asset(self):
        """Test asset retrieval."""
        # Register asset first
        self.catalog.register_asset(
            name="test_asset",
            path="/data/test",
            format="delta",
            description="Test asset",
            owner="test_user",
            auto_discover_schema=False,
        )

        # Retrieve asset
        asset = self.catalog.get_asset("test_asset")
        assert asset is not None
        assert asset.name == "test_asset"

        # Test non-existent asset
        assert self.catalog.get_asset("non_existent") is None

    def test_list_assets_with_filters(self):
        """Test asset listing with filters."""
        # Register multiple assets
        self.catalog.register_asset(
            name="asset1",
            path="/data/1",
            format="parquet",
            description="Asset 1",
            owner="user1",
            classification="public",
            tags=["tag1"],
            auto_discover_schema=False,
        )
        self.catalog.register_asset(
            name="asset2",
            path="/data/2",
            format="delta",
            description="Asset 2",
            owner="user2",
            classification="internal",
            tags=["tag2"],
            auto_discover_schema=False,
        )
        self.catalog.register_asset(
            name="asset3",
            path="/data/3",
            format="parquet",
            description="Asset 3",
            owner="user1",
            classification="public",
            tags=["tag1", "tag2"],
            auto_discover_schema=False,
        )

        # Test filter by classification
        public_assets = self.catalog.list_assets(classification="public")
        assert len(public_assets) == 2

        # Test filter by owner
        user1_assets = self.catalog.list_assets(owner="user1")
        assert len(user1_assets) == 2

        # Test filter by tags
        tag1_assets = self.catalog.list_assets(tags=["tag1"])
        assert len(tag1_assets) == 2

        # Test multiple tag filter
        both_tags_assets = self.catalog.list_assets(tags=["tag1", "tag2"])
        assert len(both_tags_assets) == 1
        assert both_tags_assets[0].name == "asset3"

    def test_update_asset(self):
        """Test asset updates."""
        # Register asset first
        self.catalog.register_asset(
            name="update_test",
            path="/data/update",
            format="parquet",
            description="Original description",
            owner="user1",
            tags=["original"],
            auto_discover_schema=False,
        )

        # Update asset
        updated_asset = self.catalog.update_asset(
            name="update_test",
            description="Updated description",
            tags=["updated", "new"],
            classification="confidential",
        )

        assert updated_asset is not None
        assert updated_asset.description == "Updated description"
        assert "updated" in updated_asset.tags
        assert "new" in updated_asset.tags
        assert updated_asset.classification == "confidential"

    def test_search_assets(self):
        """Test asset search functionality."""
        # Register assets with searchable content
        self.catalog.register_asset(
            name="customer_data",
            path="/data/customers",
            format="delta",
            description="Customer information table",
            owner="data_team",
            tags=["customers", "pii"],
            auto_discover_schema=False,
        )
        self.catalog.register_asset(
            name="order_data",
            path="/data/orders",
            format="parquet",
            description="Order transaction data",
            owner="analytics_team",
            tags=["orders", "transactions"],
            auto_discover_schema=False,
        )

        # Search by name
        customer_results = self.catalog.search_assets("customer")
        assert len(customer_results) == 1
        assert customer_results[0].name == "customer_data"

        # Search by description
        transaction_results = self.catalog.search_assets("transaction")
        assert len(transaction_results) == 1
        assert transaction_results[0].name == "order_data"

        # Search by tag
        pii_results = self.catalog.search_assets("pii")
        assert len(pii_results) == 1
        assert pii_results[0].name == "customer_data"

    def test_catalog_stats(self):
        """Test catalog statistics."""
        # Register assets with different classifications and formats
        self.catalog.register_asset(
            name="asset1",
            path="/data/1",
            format="parquet",
            description="Asset 1",
            owner="user1",
            classification="public",
            auto_discover_schema=False,
        )
        self.catalog.register_asset(
            name="asset2",
            path="/data/2",
            format="delta",
            description="Asset 2",
            owner="user2",
            classification="internal",
            auto_discover_schema=False,
        )
        self.catalog.register_asset(
            name="asset3",
            path="/data/3",
            format="parquet",
            description="Asset 3",
            owner="user3",
            classification="public",
            auto_discover_schema=False,
        )

        stats = self.catalog.get_catalog_stats()

        assert stats["total_assets"] == 3
        assert stats["by_classification"]["public"] == 2
        assert stats["by_classification"]["internal"] == 1
        assert stats["by_format"]["parquet"] == 2
        assert stats["by_format"]["delta"] == 1


class TestDataLineageTracker:
    """Test data lineage tracking functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.tracker = DataLineageTracker(lineage_path=self.temp_dir)

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)

    def test_track_event(self):
        """Test lineage event tracking."""
        event_id = self.tracker.track_event(
            event_type=LineageEventType.TRANSFORM,
            source_assets=["table_a", "table_b"],
            target_assets=["table_c"],
            transformation="JOIN on customer_id",
            job_id="job_123",
            user="analyst_user",
            metadata={"job_type": "ETL"},
        )

        assert event_id is not None
        assert len(self.tracker.events) == 1

        event = self.tracker.events[0]
        assert event.event_type == LineageEventType.TRANSFORM
        assert event.source_assets == ["table_a", "table_b"]
        assert event.target_assets == ["table_c"]
        assert event.transformation == "JOIN on customer_id"

        # Check that nodes and edges were created
        assert "table_a" in self.tracker.nodes
        assert "table_b" in self.tracker.nodes
        assert "table_c" in self.tracker.nodes
        assert "table_a->table_c" in self.tracker.edges
        assert "table_b->table_c" in self.tracker.edges

    def test_upstream_lineage(self):
        """Test upstream lineage tracking."""
        # Create a lineage chain: A -> B -> C -> D
        self.tracker.track_event(
            LineageEventType.TRANSFORM,
            ["table_a"],
            ["table_b"],
            transformation="filter",
            user="user1",
        )
        self.tracker.track_event(
            LineageEventType.TRANSFORM,
            ["table_b"],
            ["table_c"],
            transformation="aggregate",
            user="user1",
        )
        self.tracker.track_event(
            LineageEventType.TRANSFORM,
            ["table_c"],
            ["table_d"],
            transformation="join",
            user="user1",
        )

        # Get upstream lineage for table_d
        upstream = self.tracker.get_upstream_lineage("table_d", max_depth=5)

        assert upstream["asset"] == "table_d"
        assert len(upstream["upstream"]) == 1  # table_c

        # Check nested upstream
        table_c_upstream = upstream["upstream"][0]
        assert table_c_upstream["asset"] == "table_c"
        assert len(table_c_upstream["upstream"]) == 1  # table_b

        table_b_upstream = table_c_upstream["upstream"][0]
        assert table_b_upstream["asset"] == "table_b"
        assert len(table_b_upstream["upstream"]) == 1  # table_a

    def test_downstream_lineage(self):
        """Test downstream lineage tracking."""
        # Create a lineage chain: A -> B -> C -> D
        self.tracker.track_event(
            LineageEventType.TRANSFORM,
            ["table_a"],
            ["table_b"],
            transformation="filter",
            user="user1",
        )
        self.tracker.track_event(
            LineageEventType.TRANSFORM,
            ["table_b"],
            ["table_c"],
            transformation="aggregate",
            user="user1",
        )
        self.tracker.track_event(
            LineageEventType.TRANSFORM,
            ["table_c"],
            ["table_d"],
            transformation="join",
            user="user1",
        )

        # Get downstream lineage for table_a
        downstream = self.tracker.get_downstream_lineage("table_a", max_depth=5)

        assert downstream["asset"] == "table_a"
        assert len(downstream["downstream"]) == 1  # table_b

        # Check nested downstream
        table_b_downstream = downstream["downstream"][0]
        assert table_b_downstream["asset"] == "table_b"
        assert len(table_b_downstream["downstream"]) == 1  # table_c

    def test_impact_analysis(self):
        """Test impact analysis."""
        # Create a more complex lineage graph
        self.tracker.track_event(
            LineageEventType.TRANSFORM, ["source"], ["intermediate1"], user="user1"
        )
        self.tracker.track_event(
            LineageEventType.TRANSFORM, ["source"], ["intermediate2"], user="user1"
        )
        self.tracker.track_event(
            LineageEventType.TRANSFORM, ["intermediate1"], ["final1"], user="user1"
        )
        self.tracker.track_event(
            LineageEventType.TRANSFORM, ["intermediate2"], ["final2"], user="user1"
        )

        # Analyze impact of changing source
        impact = self.tracker.get_impact_analysis("source")

        assert impact["source_asset"] == "source"
        assert (
            impact["total_impacted_assets"] == 4
        )  # intermediate1, intermediate2, final1, final2
        assert "intermediate1" in impact["impacted_assets"]
        assert "intermediate2" in impact["impacted_assets"]
        assert "final1" in impact["impacted_assets"]
        assert "final2" in impact["impacted_assets"]

    def test_lineage_summary(self):
        """Test lineage summary statistics."""
        # Add some events
        self.tracker.track_event(LineageEventType.READ, [], ["table_a"], user="user1")
        self.tracker.track_event(
            LineageEventType.TRANSFORM, ["table_a"], ["table_b"], user="user1"
        )
        self.tracker.track_event(LineageEventType.WRITE, ["table_b"], [], user="user1")

        summary = self.tracker.get_lineage_summary()

        assert summary["total_events"] == 3
        assert summary["total_nodes"] == 2  # table_a, table_b
        assert summary["total_edges"] == 1  # table_a -> table_b
        assert summary["event_types"][LineageEventType.READ.value] == 1
        assert summary["event_types"][LineageEventType.TRANSFORM.value] == 1
        assert summary["event_types"][LineageEventType.WRITE.value] == 1


class TestPrivacyManager:
    """Test privacy management functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.privacy_manager = PrivacyManager(privacy_path=self.temp_dir)

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)

    def test_record_consent(self):
        """Test consent recording."""
        consent_id = self.privacy_manager.record_consent(
            user_id="user123",
            consent_type=ConsentType.MARKETING,
            granted=True,
            source="website",
            version="1.0",
            metadata={"campaign": "email_signup"},
        )

        assert consent_id is not None
        assert len(self.privacy_manager.consent_records) == 1

        record = self.privacy_manager.consent_records[0]
        assert record.user_id == "user123"
        assert record.consent_type == ConsentType.MARKETING
        assert record.granted is True
        assert record.source == "website"

    def test_get_user_consent(self):
        """Test user consent retrieval."""
        # Record multiple consents
        self.privacy_manager.record_consent("user123", ConsentType.MARKETING, True)
        self.privacy_manager.record_consent("user123", ConsentType.ANALYTICS, False)
        self.privacy_manager.record_consent(
            "user123", ConsentType.PERSONALIZATION, True
        )

        consent_status = self.privacy_manager.get_user_consent("user123")

        assert consent_status[ConsentType.MARKETING] is True
        assert consent_status[ConsentType.ANALYTICS] is False
        assert consent_status[ConsentType.PERSONALIZATION] is True

    def test_submit_data_subject_request(self):
        """Test data subject request submission."""
        request_id = self.privacy_manager.submit_data_subject_request(
            user_id="user123",
            request_type=DataSubjectRight.ERASURE,
            requested_by="user123",
            details="Please delete all my personal data",
            metadata={"verification_method": "email"},
        )

        assert request_id is not None
        assert len(self.privacy_manager.subject_requests) == 1

        request = self.privacy_manager.subject_requests[0]
        assert request.user_id == "user123"
        assert request.request_type == DataSubjectRight.ERASURE
        assert request.status == "pending"
        assert request.details == "Please delete all my personal data"

    @patch("src.data_governance.privacy.sha2")
    @patch("src.data_governance.privacy.col")
    @patch("src.data_governance.privacy.SparkSession")
    def test_anonymize_dataframe(self, mock_spark_session, mock_col, mock_sha2):
        """Test DataFrame anonymization."""
        # col/sha2 need an active SparkContext; the DataFrame here is a mock
        # Mock DataFrame and its methods
        mock_df = Mock()
        mock_df.columns = ["user_id", "email", "name", "age"]
        mock_df.withColumn.return_value = mock_df
        mock_df.drop.return_value = mock_df

        # Test hash anonymization
        result_df = self.privacy_manager.anonymize_dataframe(
            mock_df, anonymization_fields=["email", "name"], method="hash"
        )

        # Verify withColumn was called for each field
        assert mock_df.withColumn.call_count == 2

        # Test remove anonymization
        mock_df.reset_mock()
        result_df = self.privacy_manager.anonymize_dataframe(
            mock_df, anonymization_fields=["email"], method="remove"
        )

        # Verify drop was called
        mock_df.drop.assert_called_once_with("email")

    def test_privacy_report(self):
        """Test privacy report generation."""
        # Add some test data
        self.privacy_manager.record_consent("user1", ConsentType.MARKETING, True)
        self.privacy_manager.record_consent("user2", ConsentType.MARKETING, False)
        self.privacy_manager.record_consent("user1", ConsentType.ANALYTICS, True)

        self.privacy_manager.submit_data_subject_request(
            "user1", DataSubjectRight.ACCESS, "user1"
        )
        self.privacy_manager.submit_data_subject_request(
            "user2", DataSubjectRight.ERASURE, "user2"
        )

        report = self.privacy_manager.generate_privacy_report()

        assert "consent_statistics" in report
        assert "request_statistics" in report
        assert "recent_activity" in report
        assert "compliance_status" in report

        # Check consent statistics
        marketing_stats = report["consent_statistics"]["marketing"]
        assert marketing_stats["granted"] == 1
        assert marketing_stats["denied"] == 1
        assert marketing_stats["total"] == 2

        # Check request statistics
        access_stats = report["request_statistics"]["access"]
        assert access_stats["total"] == 1
        assert access_stats["pending"] == 1


class TestDataQualityMonitor:
    """Test data quality monitoring functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.quality_monitor = DataQualityMonitor(quality_path=self.temp_dir)

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)

    def test_create_rule(self):
        """Test quality rule creation."""
        rule_id = self.quality_monitor.create_rule(
            name="Completeness Check",
            description="Check that all required fields are complete",
            check_type=QualityCheckType.COMPLETENESS,
            dataset="customer_data",
            column="email",
            threshold=0.95,
            severity=AlertSeverity.WARNING,
            created_by="data_engineer",
        )

        assert rule_id is not None
        assert rule_id in self.quality_monitor.rules

        rule = self.quality_monitor.rules[rule_id]
        assert rule.name == "Completeness Check"
        assert rule.check_type == QualityCheckType.COMPLETENESS
        assert rule.dataset == "customer_data"
        assert rule.column == "email"
        assert rule.threshold == 0.95
        assert rule.enabled is True

    @patch("src.data_governance.quality.col")
    @patch("src.data_governance.quality.SparkSession")
    def test_completeness_check(self, mock_spark_session, mock_col):
        """Test completeness quality check."""
        # col() needs an active SparkContext; the DataFrame here is a mock
        # Create a rule
        rule_id = self.quality_monitor.create_rule(
            name="Email Completeness",
            description="Check email completeness",
            check_type=QualityCheckType.COMPLETENESS,
            dataset="test_data",
            column="email",
            threshold=0.9,
        )

        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_df.filter.return_value.count.return_value = 10  # 10 null values

        # Mock Spark session
        mock_spark = Mock()
        mock_spark.table.return_value = mock_df
        self.quality_monitor.spark = mock_spark

        # Run quality checks
        results = self.quality_monitor.run_quality_checks("test_data", [rule_id])

        assert len(results) == 1
        result = results[0]
        assert result.rule_id == rule_id
        assert result.check_type == QualityCheckType.COMPLETENESS
        assert result.score == 0.9  # 90 complete out of 100
        assert result.passed is True  # 0.9 >= 0.9 threshold
        assert result.total_records == 100
        assert result.failed_records == 10

    @patch("src.data_governance.quality.SparkSession")
    def test_uniqueness_check(self, mock_spark_session):
        """Test uniqueness quality check."""
        # Create a uniqueness rule
        rule_id = self.quality_monitor.create_rule(
            name="ID Uniqueness",
            description="Check ID uniqueness",
            check_type=QualityCheckType.UNIQUENESS,
            dataset="test_data",
            column="id",
            threshold=1.0,
        )

        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_df.select.return_value.distinct.return_value.count.return_value = (
            95  # 5 duplicates
        )

        # Mock Spark session
        mock_spark = Mock()
        mock_spark.table.return_value = mock_df
        self.quality_monitor.spark = mock_spark

        # Run quality checks
        results = self.quality_monitor.run_quality_checks("test_data", [rule_id])

        assert len(results) == 1
        result = results[0]
        assert result.check_type == QualityCheckType.UNIQUENESS
        assert result.score == 0.95  # 95 unique out of 100
        assert result.passed is False  # 0.95 < 1.0 threshold
        assert result.failed_records == 5

    def test_quality_dashboard(self):
        """Test quality dashboard data generation."""
        # Create some test rules and results
        rule_id = self.quality_monitor.create_rule(
            name="Test Rule",
            description="Test rule",
            check_type=QualityCheckType.COMPLETENESS,
            dataset="test_data",
            threshold=0.9,
        )

        # Manually add some results
        import uuid

        from src.data_governance.quality import QualityResult

        result1 = QualityResult(
            check_id=str(uuid.uuid4()),
            rule_id=rule_id,
            dataset="test_data",
            column="test_column",
            check_type=QualityCheckType.COMPLETENESS,
            timestamp=datetime.now(),
            score=0.95,
            passed=True,
            threshold=0.9,
            actual_value=0.95,
            total_records=100,
            failed_records=5,
            details={},
        )

        result2 = QualityResult(
            check_id=str(uuid.uuid4()),
            rule_id=rule_id,
            dataset="test_data",
            column="test_column",
            check_type=QualityCheckType.COMPLETENESS,
            timestamp=datetime.now(),
            score=0.85,
            passed=False,
            threshold=0.9,
            actual_value=0.85,
            total_records=100,
            failed_records=15,
            details={},
        )

        self.quality_monitor.results = [result1, result2]

        dashboard = self.quality_monitor.get_quality_dashboard("test_data")

        assert "overall_quality_score" in dashboard
        assert dashboard["overall_quality_score"] == pytest.approx(
            0.9
        )  # Average of 0.95 and 0.85
        assert dashboard["total_checks"] == 2
        assert dashboard["passed_checks"] == 1
        assert dashboard["failed_checks"] == 1


class TestAccessAuditor:
    """Test access auditing functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.auditor = AccessAuditor(audit_path=self.temp_dir)

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)

    def test_log_access_event(self):
        """Test access event logging."""
        event_id = self.auditor.log_access_event(
            event_type=AccessEventType.DATA_ACCESS,
            user_id="user123",
            user_role="analyst",
            resource="customer_data",
            resource_type="table",
            action="read",
            source_ip="192.168.1.100",
            user_agent="Mozilla/5.0",
            session_id="session_123",
            success=True,
            details={"query": "SELECT * FROM customers LIMIT 10"},
        )

        assert event_id is not None
        assert len(self.auditor.events) == 1

        event = self.auditor.events[0]
        assert event.event_type == AccessEventType.DATA_ACCESS
        assert event.user_id == "user123"
        assert event.user_role == "analyst"
        assert event.resource == "customer_data"
        assert event.success is True
        assert event.risk_score >= 0.0

    def test_grant_permission(self):
        """Test permission granting."""
        permission_id = self.auditor.grant_permission(
            user_id="user123",
            resource="customer_data",
            resource_type="table",
            permissions={"read", "write"},
            granted_by="admin",
            expires_at=datetime.now() + timedelta(days=30),
        )

        assert permission_id is not None
        assert "user123" in self.auditor.permissions
        assert len(self.auditor.permissions["user123"]) == 1

        permission = self.auditor.permissions["user123"][0]
        assert permission.resource == "customer_data"
        assert "read" in permission.permissions
        assert "write" in permission.permissions
        assert permission.granted_by == "admin"

    def test_check_permission(self):
        """Test permission checking."""
        # Grant permission first
        self.auditor.grant_permission(
            user_id="user123",
            resource="customer_data",
            resource_type="table",
            permissions={"read"},
            granted_by="admin",
        )

        # Check valid permission
        has_permission = self.auditor.check_permission(
            user_id="user123", resource="customer_data", action="read"
        )
        assert has_permission is True

        # Check invalid permission
        has_permission = self.auditor.check_permission(
            user_id="user123", resource="customer_data", action="write"
        )
        assert has_permission is False

        # Check for different user
        has_permission = self.auditor.check_permission(
            user_id="user456", resource="customer_data", action="read"
        )
        assert has_permission is False

    def test_risk_scoring(self):
        """Test risk score calculation."""
        # Normal access during business hours
        normal_event_id = self.auditor.log_access_event(
            event_type=AccessEventType.DATA_ACCESS,
            user_id="user123",
            user_role="analyst",
            resource="analytics_data",
            resource_type="table",
            action="read",
            source_ip="192.168.1.100",
            user_agent="Mozilla/5.0",
            session_id="session_123",
            success=True,
        )

        normal_event = self.auditor.events[0]
        normal_risk = normal_event.risk_score

        # Failed access (higher risk)
        failed_event_id = self.auditor.log_access_event(
            event_type=AccessEventType.FAILED_ACCESS,
            user_id="user123",
            user_role="analyst",
            resource="restricted_data",
            resource_type="table",
            action="read",
            source_ip="203.0.113.1",  # External IP
            user_agent="Mozilla/5.0",
            session_id="session_124",
            success=False,
        )

        failed_event = self.auditor.events[1]
        failed_risk = failed_event.risk_score

        # Failed access should have higher risk
        assert failed_risk > normal_risk
        assert failed_risk > 0.5  # Should be significantly risky

    def test_user_access_report(self):
        """Test user access report generation."""
        # Log some events for a user
        self.auditor.log_access_event(
            AccessEventType.LOGIN,
            "user123",
            "analyst",
            "system",
            "auth",
            "login",
            "192.168.1.100",
            "Mozilla/5.0",
            "session_1",
            True,
        )
        self.auditor.log_access_event(
            AccessEventType.DATA_ACCESS,
            "user123",
            "analyst",
            "customer_data",
            "table",
            "read",
            "192.168.1.100",
            "Mozilla/5.0",
            "session_1",
            True,
        )
        self.auditor.log_access_event(
            AccessEventType.FAILED_ACCESS,
            "user123",
            "analyst",
            "restricted_data",
            "table",
            "read",
            "192.168.1.100",
            "Mozilla/5.0",
            "session_1",
            False,
        )

        report = self.auditor.get_user_access_report("user123", days=30)

        assert report["user_id"] == "user123"
        assert report["access_statistics"]["total_accesses"] == 3
        assert report["access_statistics"]["successful_accesses"] == 2
        assert report["access_statistics"]["failed_accesses"] == 1
        assert report["access_statistics"]["success_rate"] == 2 / 3

        assert "customer_data" in report["resources_accessed"]
        assert "restricted_data" in report["resources_accessed"]

    def test_compliance_report(self):
        """Test compliance report generation."""
        # Log events for multiple users
        self.auditor.log_access_event(
            AccessEventType.LOGIN,
            "user1",
            "analyst",
            "system",
            "auth",
            "login",
            "192.168.1.100",
            "Mozilla/5.0",
            "session_1",
            True,
        )
        self.auditor.log_access_event(
            AccessEventType.DATA_ACCESS,
            "user2",
            "admin",
            "customer_data",
            "table",
            "read",
            "192.168.1.101",
            "Mozilla/5.0",
            "session_2",
            True,
        )
        self.auditor.log_access_event(
            AccessEventType.FAILED_ACCESS,
            "user1",
            "analyst",
            "restricted_data",
            "table",
            "read",
            "192.168.1.100",
            "Mozilla/5.0",
            "session_1",
            False,
        )

        report = self.auditor.get_compliance_report(days=30)

        assert "overview" in report
        assert "access_patterns" in report
        assert "security_summary" in report
        assert "compliance_metrics" in report

        overview = report["overview"]
        assert overview["total_events"] == 3
        assert overview["unique_users"] == 2
        assert overview["successful_accesses"] == 2
        assert overview["failed_accesses"] == 1


if __name__ == "__main__":
    pytest.main([__file__])
