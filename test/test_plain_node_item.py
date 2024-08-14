from collections.abc import Sequence
from uuid import UUID

import pytest
from psycopg import Cursor

from closure.closure import (
    Inode,
    clsr_delete_descendants,
    clsr_delete_node,
    clsr_insert,
    clsr_len,
    clsr_select_children_wpath,
    clsr_select_descendants_wpath,
)
from closure.db import bootstrap


def populate_tree(cur: Cursor, nodes: Sequence[Inode], owner: UUID):

    (id0,) = clsr_insert(cur, owner, None, nodes[0])
    (id1,) = clsr_insert(cur, owner, id0, nodes[1])
    (id2,) = clsr_insert(cur, owner, id0, nodes[2])
    (id3,) = clsr_insert(cur, owner, id1, nodes[3])
    (id4,) = clsr_insert(cur, owner, id1, nodes[4])
    (id5,) = clsr_insert(cur, owner, id2, nodes[5])
    (id6,) = clsr_insert(cur, owner, id2, nodes[6])

    return [
        id0,
        id1,
        id2,
        id3,
        id4,
        id5,
        id6,
    ]


n = 7


def make_node(i: int, name: str = "NODE") -> Inode:
    return Inode(id=None, name=f"{name}{i}", template=None, node_type="node")


nodes = [make_node(i) for i in range(n)]


@pytest.fixture(scope="function")  # , params=[None, "USER"])
def pack():  # request: pytest.FixtureRequest):

    with bootstrap() as conn:

        cur = conn.cursor(
            # row_factory=class_row(Inode)
        )

        owner = UUID("3c07ee61-4fc0-44ca-b2ad-e6820f614f74")
        try:
            ids = populate_tree(cur, nodes, owner)

            yield cur, owner, ids

        except Exception:
            conn.rollback()
            # raise e
        finally:
            try:
                cur.execute("DELETE FROM inode")  # Cleanup the test data
                conn.commit()  # Ensure cleanup changes are committed
            except Exception:
                conn.rollback()  # Rollback if cleanup fails (optional)
            finally:
                cur.close()  # Close cursor
                conn.close()  # Close connection


def test_len(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    add_items(cur, owner, ids)
    (len_,) = clsr_len(cur, owner)
    assert len_ == 21


def add_items(cur: Cursor, owner: UUID, ids: Sequence[UUID]):

    items = [
        Inode(id=None, name=f"ITEM{i}", template=None, node_type="item")
        for i in range(n * 2)
    ]

    clsr_insert(cur, owner, ids[0], items[0])
    clsr_insert(cur, owner, ids[0], items[1])
    clsr_insert(cur, owner, ids[1], items[2])
    clsr_insert(cur, owner, ids[1], items[3])
    clsr_insert(cur, owner, ids[2], items[4])
    clsr_insert(cur, owner, ids[2], items[5])
    clsr_insert(cur, owner, ids[3], items[6])
    clsr_insert(cur, owner, ids[3], items[7])
    clsr_insert(cur, owner, ids[4], items[8])
    clsr_insert(cur, owner, ids[4], items[9])
    clsr_insert(cur, owner, ids[5], items[10])
    clsr_insert(cur, owner, ids[5], items[11])
    clsr_insert(cur, owner, ids[6], items[12])
    clsr_insert(cur, owner, ids[6], items[13])


def test_select_children_item(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack
    add_items(cur, owner, ids)

    rows = clsr_select_children_wpath(cur, owner, ids[1])

    assert sorted([x[0][1] for x in rows]) == sorted(
        ["ITEM2", "ITEM3", "NODE3", "NODE4"]
    )


def test_select_descendants_item(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack
    add_items(cur, owner, ids)

    rows = clsr_select_descendants_wpath(cur, owner, ids[1])
    assert len(rows) == 8  # type: ignore


def test_delete_item(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack
    add_items(cur, owner, ids)

    clsr_delete_node(cur, owner, ids[3])

    rows = clsr_select_descendants_wpath(cur, owner, ids[1])
    assert len(rows) == 7  # type: ignore


def test_delete_descendants_item(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack
    add_items(cur, owner, ids)

    clsr_delete_descendants(cur, owner, ids[3])

    rows = clsr_select_descendants_wpath(cur, owner, ids[1])
    assert len(rows) == 5  # type: ignore
