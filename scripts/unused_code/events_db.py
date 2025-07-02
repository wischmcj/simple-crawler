
@dataclass
class Event:
    event_stream: str
    event_type_id: str
    event_source: str
    event_action: str
    event_dtm: str
    event_tags: str | None = None


class EventLogger:
    def __init__(self, seed_url: str, max_pages: int, delay: float):
        self.event_db = EventTable()
        self.run_db = RunTable()
        self.seed_url = seed_url
        self.max_pages = max_pages
        self.delay = delay

    def log_event(
        self,
        event_stream: str,
        event_type: str,
        event_source: str,
        event_action: str,
        event_tags: str = None,
    ):
        """Helper method to log events to the event database"""
        event = Event(
            event_stream=event_stream,
            event_type_id=event_type,
            event_source=event_source,
            event_action=event_action,
            event_dtm=datetime.now().isoformat(),
            event_tags=event_tags,
        )
        self.event_db.add_event(event)

    def start_crawl(self):
        """Initialize crawl and create run record"""
        self.run_id = self.run_db.start_run(
            seed_url=self.seed_url, max_pages=self.max_pages, delay=self.delay
        )
        self.log_event(
            "crawler",
            "crawl_start",
            self.seed_url,
            f"Starting crawl with max {self.max_pages} pages",
        )

    def update_crawl_status(
        self, pages_crawled: int, status: str = None, error: str = None
    ):
        """Update run status during crawling"""
        self.run_db.update_run(
            self.run_id, pages_crawled=pages_crawled, status=status, error=error
        )
        if status:
            self.log_event(
                "crawler", "status_update", self.seed_url, f"Crawl status: {status}"
            )
        if error:
            self.log_event("crawler", "error", self.seed_url, f"Crawl error: {error}")

    def complete_crawl(self, status: str = "completed"):
        """Mark crawl as complete"""
        self.run_db.complete_run(self.run_id, status)
        self.log_event(
            "crawler",
            "crawl_complete",
            self.seed_url,
            f"Crawl finished with status: {status}",
        )
class EventTable:
    def __init__(self, db_file, logger=None):
        self.connection = sqlite3.connect(db_file)
        self.cursor = self.connection.cursor()
        self.create_table()
        self.columns = [
            "event_id",
            "event_stream",
            "event_type_id",
            "event_source",
            "event_action",
            "event_dtm",
            "event_tags",
        ]
        self.logger = logger or get_logger(__name__)

    def create_table(self):
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS events (
                                run_id INTEGER,
                                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                event_stream TEXT,
                                event_type_id TEXT,
                                event_source TEXT,
                                event_action TEXT,
                                event_dtm TEXT,
                                event_tags TEXT NULL
                            );"""
        )
        self.connection.commit()

    def add_event(self, event):
        self.cursor.execute(
            """INSERT INTO events (event_stream, event_type_id, event_source, event_action, event_dtm, event_tags)
                               VALUES (?, ?, ?, ?, ?, ?);""",
            (
                event.event_stream,
                event.event_type_id,
                event.event_source,
                event.event_action,
                event.event_dtm,
                event.event_tags,
            ),
        )
        self.connection.commit()

    def update_event(self, event_id, new_event):
        """Update an event in the database
        This function probably shouldn't be used, events should be immutable
        """
        self.cursor.execute(
            """UPDATE events SET
                                event_stream = ?,
                                event_type_id = ?,
                                event_source = ?,
                                event_action = ?,
                                event_dtm = ?,
                                event_tags = ?
                            WHERE event_id = ?;""",
            (
                new_event.event_stream,
                new_event.event_type_id,
                new_event.event_source,
                new_event.event_action,
                new_event.event_dtm,
                new_event.event_tags,
                event_id,
            ),
        )
        self.connection.commit()

    def delete_event(self, event_id):
        """Delete an event from the database
        This function probably shouldn't be used, events should be immutable
        """
        self.cursor.execute("DELETE FROM events WHERE event_id = ?;", (event_id,))
        self.connection.commit()

    def get_all_events(self):
        self.cursor.execute("SELECT * FROM events;")
        rows = self.cursor.fetchall()
        events = []
        for row in rows:
            event = Event(row[1], row[2], row[3], row[4], row[5])
            events.append(event)
        return events

    def get_event_by_attribute(self, attribute, value):
        if attribute not in self.columns:
            self.logger.warning(
                f"Attribute {attribute} not found in events columns: {self.columns}"
            )
            return None
        self.cursor.execute(f"SELECT * FROM events WHERE {attribute} = {value};")
        row = self.cursor.fetchone()
        if row is None:
            self.logger.warning(f"No event found with attribute {attribute} = {value}")
            return None
        if len(row) > 0:
            event = Event(row[1], row[2], row[3], row[4], row[5])
            return event
        else:
            self.logger.warning(f"No event found with attribute {attribute} = {value}")
            return None

    def get_events(self, date):
        self.cursor.execute("SELECT * FROM events WHERE date = ?;", (date,))
        rows = self.cursor.fetchall()
        events = []
        for row in rows:
            event = Event(row[1], row[2], row[3], row[4], row[5])
            events.append(event)
        return events

    def close(self):
        self.connection.close()