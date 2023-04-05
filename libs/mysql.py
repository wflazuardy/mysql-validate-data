import re

from loguru import logger
import pymysql

from config.env import MYSQL_HOST
from config.exclusion import EXCLUDE_TABLE_LIST


class MySQLConnector:
    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        db: str
    ):
        self.host = host
        self.user = user
        self.password = password
        self.db = db

    def __enter__(self):
        logger.info(f"Try making connection to `{self.db}`.")
        self.conn = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            db=self.db
        )
        logger.info("Connected.")
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.conn.close()
        logger.info("MySQL connection closed.")

    def get_table_names(self) -> list:
        with self.conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{self.db}'
            """)
            rows = cursor.fetchall()

        table_names = []
        for row in rows:
            if f"{self.db}.{row[0]}" not in EXCLUDE_TABLE_LIST \
                    or re.search("archived|snapshot", row[0]):
                table_names.append(row[0])

        return table_names

    def get_validation_data(self) -> None:
        table_names = self.get_table_names()

        raw_query = """
            SELECT
                COUNT(1) AS ct,
                MAX(updated_at) AS max_updated_at
            FROM
                {db}.{table}
            WHERE
                updated_at < DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            LIMIT
                1
        """

        for i, table_name in enumerate(table_names, 1):
            query = raw_query.format(db=self.db, table=table_name)

            with self.conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchone()
            print(f"{i}. {table_name} | {rows[0]:,} | {rows[1]}")
