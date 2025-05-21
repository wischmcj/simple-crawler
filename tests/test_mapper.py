from __future__ import annotations

from unittest.mock import Mock

import pytest
from bs4 import BeautifulSoup
from mapper import SiteMapper

from pytest_mock import MockerFixture


class MockManager(SiteMapper):
    def on_map_success(self, arg):
        pass


@pytest.fixture
def mapper(manager):
    return MockManager(manager=manager, seed_url="https://example.com")


@pytest.fixture
def sitemap_index():
    return """<?xml version="1.0" encoding="UTF-8"?>
    <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        <sitemap>
            <loc>https://example.com/sitemap1.xml</loc>
        </sitemap>
        <sitemap>
            <loc>https://example.com/sitemap2.xml</loc>
        </sitemap>
    </sitemapindex>"""


@pytest.fixture
def sitemap_content():
    return """<?xml version="1.0" encoding="UTF-8"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sit
    emap/0.9">
        <url>
            <loc>https://example.com/page1</loc>
            <priority>0.8</priority>
            <changefreq>daily</changefreq>
            <modified>2023-01-01</modified>
        </url>
    </urlset>"""


class TestSiteMapper:
    def setUp(self, manager):
        self.mock_manager = manager
        self.mock_manager.cache = Mock()
        self.mock_manager.crawl_tracker = Mock()
        self.mock_manager.db_manager = Mock()

    def test_init(self, mapper):
        assert mapper.seed_url == "https://example.com"
        assert isinstance(mapper.sitemap_indexes, dict)
        assert isinstance(mapper.sitemap_details, list)
        assert mapper.sitemap_feilds == [
            "loc",
            "priority",
            "changefreq",
            "modified",
        ]

    def test_parse_sitemap_index(self, mapper, sitemap_index):
        soup = BeautifulSoup(sitemap_index, features="lxml")
        urls = mapper.parse_sitemap_index("https://example.com/sitemap-index.xml", soup)

        assert len(urls) == 2
        assert "https://example.com/sitemap1.xml" in urls
        assert "https://example.com/sitemap2.xml" in urls

    def test_process_sitemap(self, mapper, sitemap_content):
        soup = BeautifulSoup(sitemap_content, features="lxml")
        details = mapper.process_sitemap("https://example.com/sitemap.xml", soup)

        assert details["source_url"] == "https://example.com/sitemap.xml"
        assert details["loc"] == "https://example.com/page1"
        assert details["priority"] == "0.8"
        assert details["changefreq"] == "daily"
        assert details["modified"] == "2023-01-01"
        assert details["status"] == "Success"

    def test_recurse_sitemap_with_index(self, mapper, sitemap_index):
        sm_url = "https://example.com/sitemap-index.xml"
        mapper.recurse_sitemap("https://example.com/sitemap-index.xml", sitemap_index)
        sm_one = mapper.sitemap_indexes[sm_url]
        assert len(sm_one) == 2
        assert "https://example.com/sitemap1.xml" in sm_one
        assert "https://example.com/sitemap2.xml" in sm_one

    def test_recurse_sitemap_with_urls(self, mapper, sitemap_content):
        mapper.recurse_sitemap(
            "https://example.com/sitemap.xml", sitemap_content, "root"
        )

        assert len(mapper.sitemap_details) == 1
        assert mapper.sitemap_details[0]["loc"] == "https://example.com/page1"
        assert mapper.sitemap_details[0]["priority"] == "0.8"
        assert mapper.sitemap_details[0]["status"] == "Success"

    def test_get_sitemap_urls(self, mapper, mocker: MockerFixture):
        mock_request = mocker.patch.object(mapper, "request_page")
        mock_request.return_value = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url>
                <loc>https://example.com/page1</loc>
            </url>
        </urlset>"""

        sitemap_url, indexes, details = mapper.get_sitemap_urls(
            "https://example.com/sitemap.xml"
        )

        assert sitemap_url == "https://example.com/sitemap.xml"
        assert len(details) == 1
        assert details[0]["loc"] == "https://example.com/page1"

    def test_get_sitemap(self, mapper, mocker: MockerFixture):
        mock_get_urls = mocker.patch.object(mapper, "get_sitemap_urls")
        mock_get_urls.return_value = (
            "https://example.com/sitemap.xml",
            {"root": []},
            [],
        )

        mock_read = mocker.patch.object(mapper.downloader, "read_politeness_info")
        mock_read.return_value = (["https://example.com/sitemap.xml"], None, None)

        sitemap_url, indexes, details = mapper.get_sitemap()

        assert sitemap_url == "https://example.com/sitemap.xml"
        assert indexes == {"root": []}
        assert details == []
