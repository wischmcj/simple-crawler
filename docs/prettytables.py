# events
from __future__ import annotations

from prettytable import PrettyTable

table = PrettyTable()
table.field_names = ["Source", "Trigger", "Data", "Status After", "Tracker Action"]
table.add_row(["Mapper", "On non-sitemap link found", "URL", "Started", "Collect URL"])
table.add_row(["Mapper", "On success", "sitemap_indexes, sitemap_urls", "Started", "Request downloads for each found link"])
table.add_row(["Mapper", "On failure", "any partial results", "Downloader Error", "Add seed_url to frontier"])
table.add_row(["Downloader", "On success", "status_code, content", "Downloaded", "request page parsing"])
table.add_row(["Downloader", "On forbidden", "status_code, robots.txt info", "Forbidden", "Close URL, add to dead letter queue"])
table.add_row(["Downloader", "On failure", "status_code", "Error", "Close URL, add to retry queue"])
table.add_row(["Parser", "On start", "", "Parsed", "Add to do not parse set"])
table.add_row(["Parser", "On link found", "URL", "Started", "Init URL"])
table.add_row(["Parser", "On success", "Scraped data (links)", "Completed", "Init URL"])
table.add_row(["Parser", "On failure", "Parser Error", "Started", "Add to dead letter queue"])
print(table)

|------------|---------------------------|-------------------------------|------------------|---------------------------------------|
|   Source   |          Trigger          |              Data             |   Status After   |             Tracker Action            |
|:-----------|:-------------------------:|:-----------------------------:|:----------------:|:-------------------------------------:|
|   Mapper   | On non-sitemap link found |              URL              |     Started      |              Collect URL              |
|   Mapper   |         On success        | sitemap_indexes, sitemap_urls |     Started      | Request downloads for each found link |
|   Mapper   |         On failure        |      any partial results      | Downloader Error |        Add seed_url to frontier       |
| Downloader |         On success        |      status_code, content     |    Downloaded    |          request page parsing         |
| Downloader |        On forbidden       |  status_code, robots.txt info |    Forbidden     |  Close URL, add to dead letter queue  |
| Downloader |         On failure        |          status_code          |      Error       |     Close URL, add to retry queue     |
|   Parser   |          On start         |                               |      Parsed      |        Add to do not parse set        |
|   Parser   |       On link found       |              URL              |     Started      |                Init URL               |
|   Parser   |         On success        |      Scraped data (links)     |    Completed     |                Init URL               |
|   Parser   |         On failure        |          Parser Error         |     Started      |        Add to dead letter queue       |
