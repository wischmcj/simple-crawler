from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from simple_crawler.downloader import SiteDownloader


@pytest.fixture
def mock_manager():
    manager = Mock()
    manager.crawl_tracker = Mock()
    manager.db_manager = Mock()
    return manager


@pytest.fixture
def downloader(mock_manager):
    return SiteDownloader(manager=mock_manager)


def test_on_success(downloader):
    """Test successful download handling"""
    url = "https://example.com"
    content = "<html>test</html>"
    status_code = 200

    downloader.on_success(url, content, status_code)

    downloader.crawl_tracker.update_url.assert_called_once_with(
        url,
        {
            "attrs": {"crawl_status": "downloaded", "status_code": 200},
            "content": content,
        },
    )


def test_on_failure(downloader):
    """Test failed download handling"""
    url = "https://example.com"
    crawl_status = "error"
    status_code = 404

    downloader.crawl_tracker.update_status.return_value = {"some": "data"}

    downloader.on_failure(url, crawl_status, status_code)

    downloader.crawl_tracker.update_url.assert_called_once_with(
        url, {"attrs": {"crawl_status": "error", "status_code": 404}}, close=True
    )


@patch("simple_crawler.downloader.requests")
def test_get_page_elements_disallowed(mock_requests, downloader):
    """Test getting page elements when URL is disallowed"""
    url = "https://example.com/private"

    with patch.object(downloader, "can_fetch", return_value=False):
        content, status = downloader.get_page_elements(url)

        assert content is None
        assert status == 403
        mock_requests.get.assert_not_called()
        # I had trouble mocking on_failure, so I just confirmed the contained methods were called
        downloader.crawl_tracker.update_url.assert_called_once_with(
            url,
            {"attrs": {"crawl_status": "disallowed", "status_code": 403}},
            close=True,
        )


@patch("simple_crawler.downloader.requests")
def test_get_page_elements_success(mock_requests, downloader):
    """Test getting page elements with successful request"""
    url = "https://example.com"
    response_content = "<html>test</html>"
    status_code = 200

    mock_response = Mock()
    mock_response.text = response_content
    mock_response.status_code = status_code
    mock_requests.get.return_value = mock_response

    with patch.object(downloader, "can_fetch", return_value=True):
        content, status = downloader.get_page_elements(url)

        assert content == response_content
        assert status == status_code
        # I had trouble mocking on_success, so I just confirmed the contained methods were called
        mock_requests.get.assert_called_once_with(url, timeout=1)
        downloader.crawl_tracker.update_url.assert_called_once_with(
            url,
            {
                "attrs": {"crawl_status": "downloaded", "status_code": 200},
                "content": response_content,
            },
        )
