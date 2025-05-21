from __future__ import annotations

import os


def test_manager_initialization(manager):
    """Test manager initializes with correct attributes"""
    assert manager.seed_url == "https://example.com"
    assert manager.max_pages == 10
    assert manager.retries == 3
    assert manager.is_async is True
    assert isinstance(manager.visited_urls, set)
    assert isinstance(manager.to_visit, set)
    assert isinstance(manager.listeners, list)


def test_get_run_data(manager):
    """Test get_run_data returns correct data dictionary"""
    data = manager.get_run_data()
    assert data["seed_url"] == "https://example.com"
    assert data["max_pages"] == 10
    assert data["retries"] == 3
    assert data["is_async"] is True
    assert "run_id" in data


def test_init_dirs(manager):
    """Test directory initialization"""
    assert os.path.exists(manager.data_dir)
    assert manager.data_dir.endswith(manager.run_id)
    assert manager.rdb_path.endswith("data.rdb")
    assert manager.sqlite_path.endswith(manager.db_file)
