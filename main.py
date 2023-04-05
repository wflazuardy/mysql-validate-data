import argparse
from loguru import logger
from config import env
from libs.mysql import MySQLConnector


def main() -> None:
    with MySQLConnector(
        host=env.MYSQL_HOST,
        user=env.MYSQL_USER,
        password=env.MYSQL_PASSWORD,
        db=env.MYSQL_DB
    ) as conn:
        conn.get_validation_data()


if __name__ == "__main__":
    main()
