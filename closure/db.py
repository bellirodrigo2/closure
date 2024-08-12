from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from psycopg import Connection, connect


@contextmanager
def bootstrap() -> Generator[Connection, None, None]:

    DB_HOST = "localhost"  # Use 'localhost' or the Docker network alias if running in a Docker network
    DB_PORT = "5432"  # Default PostgreSQL port
    DB_NAME = "postgres"  # Name of the database
    DB_USER = "postgres"  # PostgreSQL user
    DB_PASSWORD = "password"  # PostgreSQL password

    conn = connect(
        host=DB_HOST,
        port=DB_PORT,
        # database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    # cur = conn.cursor()  # type: ignore

    yield conn  # , cur

    conn.close()


def make_tables(conn: Connection, file: str):

    path = Path(__file__).resolve().parent / Path(file)

    with open(path, "r") as sql_file:
        sql_content = sql_file.read()

    sql_statements = sql_content.split(";")

    cur = conn.cursor()
    for statement in sql_statements:
        if statement.strip():
            cur.execute(statement)

    conn.commit()


def query_tables(conn: Connection):

    cur = conn.cursor()

    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'  -- Adjust the schema if needed
        ORDER BY table_name;
    """
    cur.execute(query)
    return cur.fetchall()


# with bootstrap() as conn:

# make_tables(conn=conn, file='sql/closure_tables.sql')

# tables = query_tables(conn=conn)
# print(tables)

# cur = conn.cursor()

# res = cur.execute('SELECT insert_node(%s, %s);', ("root1", "475c09e6-c7e7-45c6-88a3-c33eae96b93e"))
# conn.commit()
# print(res.fetchall())
