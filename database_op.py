from typing import Dict

from sqlalchemy import create_engine, Engine, Connection


class DatabaseOp(object):
    database_conf: Dict[str, Dict[str, str]] = {
        "academe_dataspider": {
            "username": "Academe_bc_r",
            "passw": "af4b4829",
            "server": "10.32.66.183",
            "database": "Academe_DataSpider",
        },
        "newhouse": {
            "username": "xzt_academe_r",
            "passw": "Pg6Cr6CprU",
            "server": "10.32.64.236",
            "database": "newhouse",
        },
        "house_test": {
            "username": "house_test_admin",
            "passw": "klS7xjs3",
            "server": "10.24.64.167",
            "database": "house_test",
        },
    }

    def __init__(self) -> None:
        pass

    def get_db_conn_url(self, db_name: str) -> str:
        """
        获取数据库连接 URL
        :return:
        """
        db_conn_info: Dict[str, str] = self.database_conf[db_name]
        conn_url: str = f"mssql+pyodbc://{db_conn_info['username']}:{db_conn_info['passw']}@{db_conn_info['server']}/{db_conn_info['database']}?charset=utf8&driver=ODBC Driver 17 for SQL Server&TrustServerCertificate=yes"

        return conn_url

    def get_db_connection(self, conn_url: str) -> Connection:
        """
        获取数据库连接
        :param conn_url: 数据库连接 URL
        :return:
        """
        engine: Engine = create_engine(conn_url)
        conn: Connection = engine.connect()
        return conn
