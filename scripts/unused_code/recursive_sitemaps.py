
    # Link Aggregation
    def process_sitemaps(
        self, sm_soup: BeautifulSoup, sm_url: str, scheme: str, index: str = None
    ) -> set[str]:
        """Extract links from sitemap.xml if available"""

        if sm_soup.sitemapindex is not None:
            sm_locs = sm_soup.sitemapindex.find_all("loc")
            sm_urls = [sm_loc.text for sm_loc in sm_locs]
            pages = [(self.request_page(sm_url), sm_url) for sm_url in sm_urls]

            for (page_contents, status_code), sm_url in pages:
                sm_soup = BeautifulSoup(page_contents, "lxml")
                details = {}
                if sm_soup is not None:
                    details = self.process_sitemaps(sm_soup, sm_url, scheme, sm_url)
                else:
                    details["status"] = status_code
                self.sitemap_indexes[sm_url].append(details)
        else:
            details = defaultdict(list)
            details["source_url"] = sm_url
            details["index"] = index
            url = sm_soup.find("url")
            if url is not None:
                try:
                    details["loc"] = url.loc.text
                    details["priority"] = url.priority
                    details["frequency"] = url.changefreq
                    details["modified"] = url.lastmod
                    details["status"] = "200"
                except Exception as e:
                    self.logger.error(f"Error processing sitemap: {e}")
                    details["status"] = "Parsing Error"
            for key, value in details.items():
                if isinstance(value, bs4.element.Tag):
                    details[key] = value.text
            self.sitemap_details.append(dict(details))

    def get_sitemap_urls(self, url: str) -> set[str]:
        """Process a sitemap index and return all URLs found"""
        scheme, netloc, _ = parse_url(url)
        for file in [  # "sitemap-index.xml",
            "sitemap.xml"
        ]:
            sitemap_url = f"{scheme}://{netloc}/{file}"
            html = self.request_page(sitemap_url)
            soup = BeautifulSoup(html, "lxml")
            if soup is not None:
                found = file
                break
        print(f"sitemap soup: {soup}")
        if found is None:
            self.logger.warning(f"No sitemap found for {url}")
            return None
        self.process_sitemaps(soup, soup.loc.text, scheme, index="root")

        with open("google_sitemap.json", "w") as f:
            json.dump(self.sitemap_details, f, default=str, indent=4)

        return self.sitemap_details