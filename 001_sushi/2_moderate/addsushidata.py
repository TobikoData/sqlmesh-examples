import argparse
import random
import typing as t
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from sqlmesh.utils.date import to_datetime, to_ds

DB_PATH = Path(Path(__file__).parent, "db/sushi-example.db")

RAW_SCHEMA = "raw"
ORDERS_TABLE = "orders"
ITEMS_TABLE = "items"
ORDER_ITEMS_TABLE = "order_items"
MARKETING_TABLE = "marketing"
DEMOGRAPHICS_TABLE = "demographics"

CUSTOMERS = list(range(0, 100))
WAITERS = list(range(0, 10))
ITEMS = [
    "Ahi",
    "Aji",
    "Amaebi",
    "Anago",
    "Aoyagi",
    "Bincho",
    "Katsuo",
    "Ebi",
    "Escolar",
    "Hamachi",
    "Hamachi Toro",
    "Hirame",
    "Hokigai",
    "Hotate",
    "Ika",
    "Ikura",
    "Iwashi",
    "Kani",
    "Kanpachi",
    "Maguro",
    "Saba",
    "Sake",
    "Sake Toro",
    "Tai",
    "Tako",
    "Tamago",
    "Tobiko",
    "Toro",
    "Tsubugai",
    "Umi Masu",
    "Unagi",
    "Uni",
]


def set_seed(dt: datetime, seed_salt: int = 0) -> None:
    """Helper function to set a seed so random choices are not so random."""
    ts = int(dt.timestamp()) + seed_salt
    random.seed(ts)
    np.random.seed(ts)


def iter_dates(start: datetime, end: datetime) -> t.Generator[datetime, None, None]:
    """Iterate through dates."""
    for i in range((end - start).days + 1):
        yield start + timedelta(days=i)


def add_raw_data(
    start: str,
    end: str = None,
    reset: bool = False,
    customers: list[int] = CUSTOMERS,
    waiters: list[int] = WAITERS,
    items: list[str] = ITEMS,
    db_path: Path = DB_PATH,
    raw_schema: str = RAW_SCHEMA,
    tbl_names: dict[str, str] = {
        "orders": ORDERS_TABLE,
        "items": ITEMS_TABLE,
        "order_items": ORDER_ITEMS_TABLE,
    },
    customer_tbl_names: dict[str, str] = {
        "marketing": MARKETING_TABLE,
        "demographics": DEMOGRAPHICS_TABLE,
    },
) -> None:
    """
    Generate raw data and add it to the example DuckDB database.
    """
    start_dt = to_datetime(start)
    end_dt = to_datetime(end) if end else start_dt

    db_path.parent.mkdir(exist_ok=True)
    if reset:
        db_path.unlink(missing_ok=True)
        Path(db_path.name + ".wal").unlink(missing_ok=True)

    with duckdb.connect(str(db_path)) as con:
        con.sql(f"CREATE SCHEMA IF NOT EXISTS {raw_schema}")

        create_customer_data(
            customers=customers,
            db_con=con,
            raw_schema=raw_schema,
            marketing_tbl=customer_tbl_names["marketing"],
            demographics_tbl=customer_tbl_names["demographics"],
        )
        add_orders_data(
            start=start_dt,
            end=end_dt,
            customers=customers,
            waiters=waiters,
            db_con=con,
            out_tbl=f"{raw_schema}.{tbl_names['orders']}",
        )
        add_items_data(
            start=start_dt,
            end=end_dt,
            items=items,
            db_con=con,
            out_tbl=f"{raw_schema}.{tbl_names['items']}",
        )
        add_order_items_data(
            start=start_dt,
            end=end_dt,
            db_con=con,
            orders_tbl=f"{raw_schema}.{tbl_names['orders']}",
            items_tbl=f"{raw_schema}.{tbl_names['items']}",
            out_tbl=f"{raw_schema}.{tbl_names['order_items']}",
        )

    return None


def add_orders_data(
    start: datetime,
    end: datetime,
    customers: list[int],
    waiters: list[int],
    db_con: duckdb.DuckDBPyConnection,
    out_tbl: str,
) -> None:
    """Generate sushi orders data and add it to the example DuckDB database."""
    db_con.sql(
        f"CREATE TABLE IF NOT EXISTS {out_tbl} (id int, customer_id int, waiter_id int, start_ts int, end_ts int, ds date)"
    )

    dfs = []
    for dt in iter_dates(start, end):
        max_id = (
            db_con.sql(
                f"""
                SELECT MAX(id) as max_id
                FROM {out_tbl}
                WHERE ds = '{to_ds(dt)}'
                """
            )
            .df()["max_id"]
            .to_list()[0]
        )
        max_id = max_id if not pd.isna(max_id) else 0
        set_seed(dt, seed_salt=max_id)

        num_orders = random.randint(10, 30)

        start_ts = [
            int((dt + timedelta(seconds=random.randint(0, 80000))).timestamp())
            for _ in range(num_orders)
        ]

        end_ts = [int(s + random.randint(0, 60 * 60)) for s in start_ts]

        dt_df = (
            pd.DataFrame(
                {
                    "customer_id": random.choices(customers, k=num_orders),
                    "waiter_id": random.choices(waiters, k=num_orders),
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "ds": to_ds(dt),
                }
            )
            .reset_index()
            .rename(columns={"index": "id"})
        )

        dt_df["id"] = range(max_id + 1, max_id + 1 + dt_df.shape[0])

        dfs.append(dt_df)

    out_df = pd.concat(dfs)  # noqa

    db_con.sql(f"INSERT INTO {out_tbl} SELECT * FROM out_df")

    return None


def add_items_data(
    start: datetime,
    end: datetime,
    items: list[str],
    db_con: duckdb.DuckDBPyConnection,
    out_tbl: str,
) -> None:
    """Generate sushi items data and add it to the example DuckDB database."""
    db_con.sql(f"CREATE TABLE IF NOT EXISTS {out_tbl} (id int, name text, price double, ds date)")

    existing_item_ds = (
        db_con.sql(
            f"""
            SELECT DISTINCT ds
            FROM {out_tbl}
            """
        )
        .df()["ds"]
        .tolist()
    )

    dfs = []
    for dt in iter_dates(start, end):
        set_seed(dt)

        if not to_ds(dt) in existing_item_ds:
            num_items = random.randint(10, len(items))
            dfs.append(
                pd.DataFrame(
                    {
                        "name": random.sample(items, num_items),
                        "price": np.random.uniform(3.0, 10.0, size=num_items).round(2),
                        "ds": to_ds(dt),
                    }
                )
                .reset_index()
                .rename(columns={"index": "id"})
            )

    if dfs:
        out_df = pd.concat(dfs)  # noqa

        db_con.sql(f"INSERT INTO {out_tbl} SELECT * FROM out_df")

    return None


def add_order_items_data(
    start: datetime,
    end: datetime,
    db_con: duckdb.DuckDBPyConnection,
    orders_tbl: str,
    items_tbl: str,
    out_tbl: str,
) -> None:
    """Generate raw order items data and add it to the example DuckDB database. Requires orders and items tables to exist."""
    db_con.sql(
        f"CREATE TABLE IF NOT EXISTS {out_tbl} (id int, order_id int, item_id int, quantity int, ds date)"
    )

    dfs = []
    for dt in iter_dates(start, end):
        orders = db_con.sql(
            f"""
            SELECT *
            FROM {orders_tbl}
            WHERE ds = '{to_ds(dt)}'
            """
        ).df()

        items = db_con.sql(
            f"""
            SELECT *
            FROM {items_tbl}
            WHERE ds = '{to_ds(dt)}'
            """
        ).df()

        existing_order_items = db_con.sql(
            f"""
            SELECT id, order_id
            FROM {out_tbl}
            WHERE ds = '{to_ds(dt)}'
            """
        ).df()
        orders_to_process = set(orders["id"].tolist()) - set(
            existing_order_items["order_id"].tolist()
        )

        max_order_item_id = 0
        if existing_order_items.shape[0] > 0:
            max_order_item_id = max(existing_order_items["id"].tolist())
        set_seed(dt, seed_salt=max_order_item_id)

        order_dfs = []
        for order_id in orders_to_process:
            n = random.randint(1, 5)

            order_dfs.append(
                pd.DataFrame(
                    {
                        "order_id": order_id,
                        "item_id": items.sample(n=n)["id"],
                        "quantity": np.random.randint(1, 10, n),
                        "ds": to_ds(dt),
                    }
                )
            )
        dt_df = (
            pd.concat(order_dfs, ignore_index=True).reset_index().rename(columns={"index": "id"})
        )

        dt_df["id"] = range(max_order_item_id + 1, max_order_item_id + 1 + dt_df.shape[0])

        dfs.append(dt_df)

    out_df = pd.concat(dfs)  # noqa

    db_con.sql(f"INSERT INTO {out_tbl} SELECT * FROM out_df")

    return None


def create_customer_data(
    db_con: duckdb.DuckDBPyConnection,
    customers: list,
    raw_schema: str,
    marketing_tbl: str,
    demographics_tbl: str,
) -> None:
    """Generate raw customer data and add it to the example DuckDB database."""
    set_seed(to_datetime(42))

    tables = (
        db_con.sql(
            f"""
        select table_name
        from information_schema.tables
        where table_schema = '{raw_schema}'
        """
        )
        .df()["table_name"]
        .tolist()
    )

    if not marketing_tbl in tables:
        marketing_df = (
            pd.DataFrame(
                {
                    "name": customers,
                    "status": random.choices(
                        ["ACTIVE", "INACTIVE"], weights=[80, 20], k=len(customers)
                    ),
                }
            )
            .reset_index()
            .rename(columns={"index": "id"})
        )

        db_con.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_schema}.{marketing_tbl} (id int, customer_id int, status text)"
        )
        db_con.sql(f"INSERT INTO {raw_schema}.{marketing_tbl} SELECT * FROM marketing_df")

    if not demographics_tbl in tables:
        demographics_df = (
            pd.DataFrame(
                {
                    "name": customers,
                    "status": random.sample(
                        [str(zip) for zip in range(10000, 11000)], k=len(customers),
                    ),
                }
            )
            .reset_index()
            .rename(columns={"index": "id"})
        )

        db_con.sql(
            f"CREATE TABLE IF NOT EXISTS {raw_schema}.{demographics_tbl} (id int, customer_id int, zip text)"
        )
        db_con.sql(f"INSERT INTO {raw_schema}.{demographics_tbl} SELECT * FROM demographics_df")

    return None

def print_file(path: str):
    with open(path, 'r') as file:
        print(file.read())


def parse_arguments() -> dict[str, str]:
    parser = argparse.ArgumentParser(description="Add data to the SQLMesh sushi example database.")

    parser.add_argument("--start", help="First day to add data for")
    parser.add_argument("--end", help="Last day to add data for")
    parser.add_argument("--reset", help="Reset database to initial state", action="store_true")

    return vars(parser.parse_args())


if __name__ == "__main__":
    args: dict[str, t.Any] = parse_arguments()
    add_raw_data(**args)
