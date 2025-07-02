import duckdbv
import dlt
import os
import sys

loc = os.path.dirname(os.path.dirname(__file__))
sys.path.append(loc)

db = duckdb.connect()
p = dlt.pipeline(
  pipeline_name="simple_crawler",
  destination=dlt.destinations.duckdb(db),
  dataset_name="url_data",
  dev_mode=False,
)

# Or if you would like to use an in-memory duckdb instance
db = duckdb.connect("../dlt/sc.duckdb")
p = pipeline_one = dlt.pipeline(
  pipeline_name="in_memory_pipeline",
  destination=dlt.destinations.duckdb(db),
  dataset_name="chess_data",
)

print(db.sql("DESCRIBE;"))