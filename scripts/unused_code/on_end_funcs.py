import json

from config.configuration import get_logger

logger = get_logger("crawler")


def on_failure(
    job, connection, type, value, traceback, continue_on_failure=False
):
    """Callback for when a job succeeds"""
    manager = job.meta["manager"]
    seed_url = job.meta["seed_url"]
    run_id = job.meta["run_id"]
    url = job.meta["url"]
    func_name = job.meta["func_name"]

    logger.info(f"{func_name} for {url} failed")
    if not continue_on_failure:
        close_url(manager, url, run_id, seed_url, error_status=True)
    else:
        # Move the url to the next function in the pipeline
        url_data = manager.cache.update_status(url, func_name)

def on_map_failure(job, connection, type, value, traceback):
    """Callback for when a site mapping job fails"""
    on_failure(job, connection, type, value, traceback, continue_on_failure=True)
    logger.warning(f"Sitemap for {job.meta['url']} not found")
    logger.warning("Defaulting to seed_url links for base seeds")

def on_download_failure(job, connection, type, value, traceback):
    """Callback for when a download job fails"""
    on_failure(job, connection, type, value, traceback, continue_on_failure=True)
    logger.warning(f"Download for {job.meta['url']} failed")


#   General on end functions to update status/save data
def on_success(job, connection, result, func_name):
    """Callback for when a job succeeds"""
    manager = job.meta["manager"]
    seed_url = job.meta["seed_url"]
    logger.info(f"{func_name} for {seed_url} succeeded")
    manager.cache.update_status(seed_url, func_name)

# Map site specific on end functions
def on_map_success(job, connection, result):
    on_success(job, connection, result, "map_site")
    manager = job.meta["manager"]
    """Callback for when a site mapping job succeeds"""
    _, sitemap_indicies, sitemap_details = result
    logger.info("Writing sitemap data to sqlite, index data to file")

    with open(f"{manager.data_dir}/sitemap_indexes.json", "w") as f:
        json.dump(sitemap_indicies, f, default=str, indent=4)

    for detail in sitemap_details:
        manager.sitemap_table.store_sitemap_data(detail)

# On end functions for download
def on_download_success(job, connection, result):
    """
    When a download success, progress the urls status,
        and enqueue the page for parsing.
    """
    # Content, request_code have been saved to cache
    on_success(job, connection, result, "download")
    manager = job.meta["manager"]
    current_url = job.meta["url"]
    new_links = result
    manager.cache.add_page_to_visit(current_url, new_links)

# Parse specific on end functions
def on_parse_success(job, connection, result):
    url, result = on_success(job, connection, result, "parse")
    """Callback for when a parse job succeeds"""
    manager = job.meta["manager"]
    run_id = job.meta["run_id"]
    url = job.meta["url"]
    seed_url = job.meta["seed_url"]

    logger.info(f"Parse job {job.id} succeeded")
    current_url, new_links = result
    manager.db_manager.store_links(seed_url, current_url, new_links)
    for link in new_links:
        manager.visit_tracker.add_page_to_visit(current_url, link)


def close_url(manager, url, run_id, seed_url):
    """Callback for when a url is closed"""
    # Pop data from cache
    data = manager.cache.close_url(url)
    if data:
        if data.status != "error":
            data["status"] = "error"
        manager.db_manager.store_url(data, run_id, seed_url)

