import os

os.chdir("./sushi")

from addsushidata import add_raw_data
from sqlmesh import Context
import sqlmesh.core.dialect as d
from sqlglot import parse_one

con = Context(paths="/users/trey/tobiko/sqlmesh-examples/sushi")

con.plan("prod", no_prompts=True, auto_apply=True)

con.run("prod", start="2023-01-06", end="2023-08-18", ignore_cron=True)

add_raw_data("2023-01-06", "2023-01-06")
