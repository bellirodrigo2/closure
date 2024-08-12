from collections.abc import Iterable
from typing import Any, Literal
from uuid import UUID

from psycopg import Cursor
from pydantic import BaseModel

NodeType = Literal["node", "item", "template"]


class IdNotFoundError(Exception): ...


class DuplicatedIdError(Exception): ...


class DuplicatedNameError(Exception): ...


class OwnershipError(Exception): ...


class Inode(BaseModel):

    id: UUID | None
    name: str
    template: UUID | None
    node_type: NodeType


def clsr_len(cur: Cursor, owner: UUID) -> Any:
    cur.execute("SELECT clsr_len(%s);", (owner,))
    return cur.fetchone()


def clsr_insert(cur: Cursor, owner: UUID, parent: UUID | None, inode: Inode) -> Any:
    cur.execute(
        "SELECT insert_node(%s, %s, %s, %s, %s);",
        (inode.name, owner, inode.node_type, inode.template, parent),
    )
    return cur.fetchone()


def clsr_select_roots(cur: Cursor, owner: UUID):
    cur.execute("SELECT select_roots(%s)", (owner,))
    return cur.fetchall()


def clsr_select_root_byid(cur: Cursor, owner: UUID, id: UUID):
    cur.execute(
        "SELECT select_root_byid(%s, %s)",
        (
            id,
            owner,
        ),
    )
    return cur.fetchone()


def clsr_get_path(cur: Cursor, owner: UUID, id: UUID) -> str:

    cur.execute(
        """
                SELECT string_agg(n.name, '.') AS path FROM inode n
                JOIN link t ON t.parent = n.id
                WHERE t.child = %s""",
        (id,),
    )
    return cur.fetchone()


def clsr_select_byid(cur: Cursor, owner: UUID, id: UUID):
    cur.execute("SELECT select_byid(%s, %s)", (id, owner))
    row = cur.fetchone()
    if row is None:
        raise IdNotFoundError(f"Id {id} not found for owner {owner}")
    return row[0]


def clsr_select_children(cur: Cursor, owner: UUID, id: UUID) -> Iterable[Any]:
    cur.execute("SELECT select_child(%s, %s);", (id, owner))
    return cur.fetchall()


def clsr_select_descendants(cur: Cursor, owner: UUID, id: UUID) -> Iterable[Any]:
    cur.execute("SELECT select_descendants(%s, %s);", (id, owner))
    return cur.fetchall()


def clsr_select_children_json(cur: Cursor, owner: UUID, id: UUID) -> Iterable[Any]:
    cur.execute("SELECT select_children_json(%s, %s);", (id, owner))
    return cur.fetchone()


def clsr_select_descendants_json(cur: Cursor, owner: UUID, id: UUID) -> Iterable[Any]:
    cur.execute("SELECT select_descendants_json(%s, %s);", (id, owner))
    return cur.fetchone()


def clsr_delete_node(cur: Cursor, owner: UUID, id: UUID) -> Any:

    cur.execute("SELECT delete_node(%s, %s);", (id, owner))

    return cur.fetchone()


def clsr_delete_descendants(cur: Cursor, owner: UUID, id: UUID) -> Any:

    cur.execute("SELECT delete_descendants(%s, %s);", (id, owner))

    return cur.fetchone()

    # def select_roots(self) -> Iterable[T]: ...
    # def select_node_root(self, id: str) -> T: ...
