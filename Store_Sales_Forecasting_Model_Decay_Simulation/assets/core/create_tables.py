from dagster_snowflake import SnowflakeResource


def get_table_creation_query(table_name: str) -> str:
    """Returns the SQL query to create the table with the given name"""
    try:
        return {
            "store_info": """
                            CREATE TABLE IF NOT EXISTS 
                            STORE_SALES_DB.STORE_SALES_SCHEMA.STORE_INFO (
                                ID INT,
                                STORE_NBR INT,
                                CITY VARCHAR(20),
                                STATE VARCHAR(20),
                                TYPE VARCHAR(5),
                                CLUSTER INT
                            );
                            """,
            "store_sales": """
                            CREATE TABLE IF NOT EXISTS 
                            STORE_SALES_DB.STORE_SALES_SCHEMA.STORE_SALES (
                                DATE TIMESTAMP_NTZ,
                                STORE_NBR INT,
                                FAMILY VARCHAR(50),
                                SALES FLOAT,
                                ONPROMOTION INT
                            );
                            """,
            "oil_prices": """
                            CREATE TABLE IF NOT EXISTS 
                            STORE_SALES_DB.STORE_SALES_SCHEMA.OIL_PRICES (
                                DATE TIMESTAMP_NTZ,
                                DCOILWTICO FLOAT
                            );
                            """,
            "transactions": """
                            CREATE TABLE IF NOT EXISTS 
                            STORE_SALES_DB.STORE_SALES_SCHEMA.TRANSACTIONS (
                                DATE TIMESTAMP_NTZ,
                                STORE_NBR INT,
                                TRANSACTIONS INT
                            );
                            """,
            "holidays": """
                            CREATE TABLE IF NOT EXISTS 
                            STORE_SALES_DB.STORE_SALES_SCHEMA.HOLIDAYS (
                                DATE TIMESTAMP_NTZ,
                                TYPE VARCHAR(10),
                                LOCALE VARCHAR(10),
                                LOCALE_NAME VARCHAR(50),
                                DESCRIPTION VARCHAR(50),
                                TRANSFERRED BOOLEAN
                            );
                            """,
            "national_holidays": """
                                    CREATE TABLE IF NOT EXISTS 
                                    STORE_SALES_DB.STORE_SALES_SCHEMA.NATIONAL_HOLIDAYS (
                                        DATE TIMESTAMP_NTZ,
                                        NATIONAL_HOLIDAY BOOLEAN
                                    );
                                    """,
            "local_holidays": """
                                CREATE TABLE IF NOT EXISTS 
                                STORE_SALES_DB.STORE_SALES_SCHEMA.LOCAL_HOLIDAYS (
                                    DATE TIMESTAMP_NTZ,
                                    CITY VARCHAR(50),
                                    LOCAL_HOLIDAY BOOLEAN
                                );
                                """,
            "regional_holidays": """
                                    CREATE TABLE IF NOT EXISTS 
                                    STORE_SALES_DB.STORE_SALES_SCHEMA.REGIONAL_HOLIDAYS (
                                        DATE TIMESTAMP_NTZ,
                                        STATE VARCHAR(50),
                                        REGIONAL_HOLIDAY BOOLEAN
                                    );
                                    """,
        }[table_name]
    except KeyError:
        raise ValueError(f"Invalid table name: {table_name}")


def create_table(snowflake_resource: SnowflakeResource, table_name: str) -> None:
    """Create the table with the given name in the snowflake database. This also
        creates the database and schema if ever it does not exist to prevent any
        errors.

    Args:
        snowflake_resource (SnowflakeResource): A Snowflake resource that
            provides a connection to the snowflake database.
        table_name (str): The name of the table to be created in the snowflake
            database. One of "store_info", "store_sales", "oil_prices",
            "transactions", "holidays", "national_holidays", "local_holidays",
            or "regional_holidays".

    NOTE:
    This function was created to prevent dagster-snowflake from creating tables with
    wrong data types because of empty dataframes.
    """
    with snowflake_resource.get_connection() as conn:

        conn.cursor().execute("CREATE DATABASE IF NOT EXISTS STORE_SALES_DB;")
        conn.cursor().execute(
            "CREATE SCHEMA IF NOT EXISTS STORE_SALES_DB.STORE_SALES_SCHEMA;"
        )

        table_creation_query = get_table_creation_query(table_name=table_name)
        conn.cursor().execute(table_creation_query)
