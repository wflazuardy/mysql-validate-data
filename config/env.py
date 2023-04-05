from os import getenv
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = getenv("MYSQL_HOST")
MYSQL_USER = getenv("MYSQL_USER")
MYSQL_PASSWORD = getenv("MYSQL_PASSWORD")
MYSQL_DB = getenv("MYSQL_DB")
