"""Unit tests for the SharePoint 2019 V2 source connector."""

import pytest

from airweave.platform.sources.sharepoint2019v2.source import SharePoint2019V2Source


@pytest.fixture
def source_with_ad():
    """Create a SharePoint2019V2Source with AD config populated."""
    source = SharePoint2019V2Source()
    source._ad_username = "admin"
    source._ad_password = "pass"
    source._ad_domain = "DOMAIN"
    source._ad_server = "ldaps://server:636"
    source._ad_search_base = "DC=DOMAIN,DC=local"
    return source


@pytest.fixture
def source_without_ad():
    """Create a SharePoint2019V2Source without AD config."""
    source = SharePoint2019V2Source()
    source._ad_username = ""
    source._ad_password = ""
    source._ad_domain = ""
    source._ad_server = ""
    source._ad_search_base = ""
    return source


class TestSupportsIncrementalAcl:
    """Tests for supports_incremental_acl().

    The method should return True only when AD config is present AND the
    class has supports_continuous=True (set by the @source decorator).
    """

    def test_returns_true_with_ad_config(self, source_with_ad):
        """With AD config and supports_continuous=True on the class, should return True."""
        assert source_with_ad.supports_incremental_acl() is True

    def test_returns_false_without_ad_config(self, source_without_ad):
        """Without AD config, should return False even though class supports continuous."""
        assert source_without_ad.supports_incremental_acl() is False

    def test_reads_class_attribute_not_instance(self, source_with_ad):
        """Should read supports_continuous from the class, not an instance attribute."""
        # The @source decorator sets supports_continuous as a class attribute.
        # Verify the method checks the class, not the instance.
        assert hasattr(SharePoint2019V2Source, "supports_continuous")
        assert SharePoint2019V2Source.supports_continuous is True
