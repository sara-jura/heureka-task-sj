import psycopg2
import psycopg2.extras
from typing import List, Dict, Any


class PostgresDAO:
    """Data access object for communication with the Postgres database"""

    def __init__(self, db: str, username: str, password: str, port: str, host: str) -> None:
        self.db = db
        self.username = username
        self.password = password
        self.port = port
        self.host = host
        self.conn = None

    def connect(self) -> None:
        self.conn = psycopg2.connect(
            dbname=self.db,
            user=self.username,
            password=self.password,
            port=self.port,
            host=self.host,
        )
        self.conn.autocommit = True

    def execute_query(self, query: str) -> None:
        """Executes non-select queries such as insert or delete"""
        cursor = self.conn.cursor()
        cursor.execute(query)

    def fetch_dicts(self, query: str) -> List[Dict[str, Any]]:
        """Returns data as a list of dicts where column names serve as the keys"""
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query)
        return cursor.fetchall()

    def count_rows(self, query: str) -> int:
        """Return the number of rows for a give 'select count()...' query"""
        cursor = self.conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0]

    def close(self) -> None:
        """ Closes the connection """
        self.conn.close()
