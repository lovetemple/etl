import logging
import allure
from ..clients.bigquery import BigQueryClient

logger = logging.getLogger(__name__)


def assert_table_exists(bq_client: BigQueryClient, table_id: str):
    """
    Asserts that a BigQuery table exists.
    """
    with allure.step(f"Assert table exists: {table_id}"):
        logger.info(f"Checking existence of table: {table_id}")
        exists = bq_client.check_table_exists(table_id)
        if not exists:
            allure.attach(
                f"Table {table_id} not found.",
                name="Error Detail",
                attachment_type=allure.attachment_type.TEXT,
            )
        assert exists, f"Table {table_id} does not exist."
        logger.info(f"Assertion passed: Table {table_id} exists.")


def assert_row_count(
    bq_client: BigQueryClient,
    table_id: str,
    expected_count: int = None,
    min_count: int = None,
):
    """
    Asserts row count matches expected value or is greater than min_count.
    """
    with allure.step(f"Assert row count for {table_id}"):
        logger.info(f"Fetching row count for {table_id}")
        actual_count = bq_client.get_row_count(table_id)
        allure.attach(
            str(actual_count),
            name="Actual Row Count",
            attachment_type=allure.attachment_type.TEXT,
        )

        if expected_count is not None:
            with allure.step(f"Verify exact count: {expected_count}"):
                assert (
                    actual_count == expected_count
                ), f"Expected {expected_count} rows in {table_id}, but found {actual_count}."

        if min_count is not None:
            with allure.step(f"Verify minimum count: {min_count}"):
                assert (
                    actual_count >= min_count
                ), f"Expected at least {min_count} rows in {table_id}, but found {actual_count}."

        logger.info(f"Assertion passed: Row count for {table_id} is {actual_count}.")


def assert_schema_contains_columns(
    bq_client: BigQueryClient, table_id: str, required_columns: list[str]
):
    """
    Asserts that the table schema contains the specified columns.
    """
    with allure.step(f"Assert schema of {table_id} contains {required_columns}"):
        logger.info(f"Fetching schema for {table_id}")
        table = bq_client.get_table(table_id)
        actual_columns = [field.name for field in table.schema]
        
        allure.attach(
            str(actual_columns),
            name="Actual Schema Columns",
            attachment_type=allure.attachment_type.TEXT,
        )
        
        missing = [col for col in required_columns if col not in actual_columns]

        if missing:
             allure.attach(
                str(missing),
                name="Missing Columns",
                attachment_type=allure.attachment_type.TEXT,
            )

        assert not missing, f"Table {table_id} is missing columns: {missing}"
        logger.info(
            f"Assertion passed: Table {table_id} contains columns {required_columns}."
        )


def assert_sql_result(bq_client: BigQueryClient, query: str, expected_rows: list[dict] | None = None):
    """
    Simpler assertion: Runs query and checks if it returns results (or matches expected).
    If expected_rows is provided, checks exact equality (order sensitive).
    If expected_rows is None, just asserts that rows > 0.
    """
    with allure.step("Assert SQL Query Result"):
        allure.attach(query, name="Query", attachment_type=allure.attachment_type.TEXT)
        logger.info(f"Running query: {query}")
        
        rows = bq_client.execute_query(query)
        logger.info(f"Query returned {len(rows)} rows.")
        
        if expected_rows is not None:
            assert rows == expected_rows, f"Expected {expected_rows}, got {rows}"
        else:
            assert len(rows) > 0, "Query returned no rows"
            
        logger.info("Assertion passed.")

def assert_data_integrity(bq_client: BigQueryClient, query: str, check_func):
    """
    Advanced assertion: Runs a function against query results.
    """
    with allure.step("Assert Data Integrity"):
        allure.attach(query, name="Validation Query", attachment_type=allure.attachment_type.TEXT)
        logger.info(f"Running data integrity query: {query}")
        
        rows = bq_client.execute_query(query)
        
        try:
            check_func(rows)
            logger.info("Assertion passed.")
        except AssertionError as e:
            logger.error(f"Assertion failed: {e}")
            allure.attach(str(e), name="Assertion Failure", attachment_type=allure.attachment_type.TEXT)
            raise e


// validators/null_checks.py
def assert_no_nulls(bq, table, column):
    sql = f"""
    SELECT COUNT(*) cnt
    FROM `{table}`
    WHERE {column} IS NULL
    """
    cnt = list(bq.query(sql))[0].cnt
    assert cnt == 0, f"Found {cnt} NULLs in {column}"

    // validators/referential_integrity.py
def assert_sat_has_hub_keys(bq, hub, satellite, key):
    sql = f"""
    SELECT COUNT(*) cnt
    FROM `{satellite}` s
    LEFT JOIN `{hub}` h
      ON s.{key} = h.{key}
    WHERE h.{key} IS NULL
    """
    cnt = list(bq.query(sql))[0].cnt
    assert cnt == 0, f"Satellite has {cnt} orphan records"


//validators/freshness.py
def assert_fresh_data(bq, table, ts_column, hours=24):
    sql = f"""
    SELECT COUNT(*) cnt
    FROM `{table}`
    WHERE {ts_column} < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    """
    cnt = list(bq.query(sql))[0].cnt
    assert cnt == 0, "Stale data detected"


//Topic has data
def assert_topic_not_empty(messages):
    assert len(messages) > 0, "Kafka topic is empty"

//Required fields exist
def assert_required_fields(messages, required_fields):
    for i, msg in enumerate(messages):
        missing = [f for f in required_fields if f not in msg]
        assert not missing, (
            f"Message {i} missing fields: {missing}"
        )

//Timestamp freshness
from datetime import datetime, timezone, timedelta

def assert_event_freshness(
    messages,
    ts_field,
    max_age_minutes=60
):
    now = datetime.now(timezone.utc)
    stale = []

    for msg in messages:
        event_ts = datetime.fromisoformat(msg[ts_field])
        if now - event_ts > timedelta(minutes=max_age_minutes):
            stale.append(msg)

    assert not stale, (
        f"{len(stale)} stale Kafka events detected"
    )


    //Optional: Business key uniqueness (sample window)
def assert_unique_keys(messages, key):
    values = [msg[key] for msg in messages]
    duplicates = set(v for v in values if values.count(v) > 1)

    assert not duplicates, (
        f"Duplicate keys found in Kafka messages: {duplicates}"
    )