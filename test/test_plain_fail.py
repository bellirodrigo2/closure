from collections.abc import Sequence
from uuid import UUID, uuid4

import pytest
from psycopg import Cursor, Error

from closure.closure import (
    IdNotFoundError,
    Inode,
    clsr_delete_descendants,
    clsr_delete_node,
    clsr_insert,
    clsr_len,
    clsr_select_byid,
    clsr_select_children,
    clsr_select_descendants,
)
from closure.db import bootstrap


def populate_tree(cur: Cursor, nodes: Sequence[Inode], owner: UUID):

    (id0,) = clsr_insert(cur, owner, None, nodes[0])
    (id1,) = clsr_insert(cur, owner, id0, nodes[1])
    (id2,) = clsr_insert(cur, owner, id0, nodes[2])
    (id3,) = clsr_insert(cur, owner, id1, nodes[3])
    (id4,) = clsr_insert(cur, owner, id1, nodes[4])
    (id5,) = clsr_insert(cur, owner, id1, nodes[5])
    (id6,) = clsr_insert(cur, owner, id2, nodes[6])
    (id7,) = clsr_insert(cur, owner, id2, nodes[7])
    (id8,) = clsr_insert(cur, owner, id2, nodes[8])
    return [
        id0,
        id1,
        id2,
        id3,
        id4,
        id5,
        id6,
        id7,
        id8,
    ]


n = 9


def make_node(i: int) -> Inode:
    return Inode(id=None, name=f"NODE{i}", template=None, node_type="node")


nodes = [make_node(i) for i in range(n)]
owner2 = UUID("9a0cd152-259a-4852-8eaf-da8c7af032d6")


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


def test_owner_low_level(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    cur.execute("CALL owner_exist(%s)", (owner,))
    cur.execute("CALL is_owner(%s, %s)", (ids[0], owner))

    with pytest.raises(expected_exception=Error):
        cur.execute("CALL is_owner(%s, %s)", (ids[0], uuid4()))

    with pytest.raises(expected_exception=Error):
        cur.execute("CALL owner_exist(%s)", (uuid4(),))


def test_len_OK(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, _ = pack
    (len_,) = clsr_len(cur, owner)
    assert len_ == n

    (len_,) = clsr_len(cur, owner2)
    assert len_ == 0


def test_len_fail_no_owner(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, _, _ = pack

    with pytest.raises(expected_exception=Error):
        clsr_len(cur, uuid4())


def test_insert_fail_no_owner(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, _, _ = pack

    with pytest.raises(expected_exception=Error):
        clsr_insert(cur, uuid4(), None, nodes[2])


def test_insert_fail_different_owner(pack: tuple[Cursor, UUID, Sequence[UUID]]):
    "Parent owner is different then owner passed in the function parameter"
    cur, _, ids = pack

    with pytest.raises(expected_exception=Error):
        clsr_insert(cur, owner2, ids[0], nodes[2])


def test_has_similar_sibling_low_level(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    cur.execute(
        "CALL has_similar_sibling(%s, %s, %s, %s)", (ids[0], "NEWNAME", "node", owner)
    )
    cur.execute(
        "CALL has_similar_sibling(%s, %s, %s, %s)", (ids[0], "NODE18", "node", owner)
    )

    with pytest.raises(expected_exception=Error):
        cur.execute(
            "CALL has_similar_sibling(%s, %s, %s, %s)", (ids[0], "NODE1", "node", owner)
        )

    with pytest.raises(expected_exception=Error):
        cur.execute(
            "CALL has_similar_sibling(%s, %s, %s, %s)", (ids[2], "NODE8", "node", owner)
        )


def test_has_root_low_level(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, _ = pack

    cur.execute("CALL has_similar_root(%s, %s, %s)", ("NODE3", "node", owner))

    with pytest.raises(expected_exception=Error):
        cur.execute("CALL has_similar_root(%s, %s, %s)", ("NODE0", "node", owner))


def test_insert_fail_same_name_sibling(pack: tuple[Cursor, UUID, Sequence[UUID]]):
    cur, owner, ids = pack

    new_node = Inode(id=None, name="NODE4", template=None, node_type="node")
    new_node2 = Inode(id=None, name="NODE8", template=None, node_type="node")

    clsr_insert(cur, owner, ids[4], new_node)

    with pytest.raises(expected_exception=Error):
        clsr_insert(cur, owner, ids[1], new_node)

    with pytest.raises(expected_exception=Error):
        clsr_insert(cur, owner, ids[2], new_node2)


def test_insert_fail_same_name_root(pack: tuple[Cursor, UUID, Sequence[UUID]]):
    cur, owner, _ = pack

    new_node = Inode(id=None, name="NODE0", template=None, node_type="node")

    with pytest.raises(expected_exception=Error):
        clsr_insert(cur, owner, None, new_node)


def test_select_byid_fail_id(pack: tuple[Cursor, UUID, Sequence[UUID]]):
    cur, _, ids = pack

    with pytest.raises(expected_exception=IdNotFoundError):
        clsr_select_byid(cur, owner2, ids[0])


def test_select_children_fail_parent_owner(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    assert clsr_select_children(cur, owner, ids[8]) == []

    with pytest.raises(expected_exception=Error):
        clsr_select_children(cur, owner2, ids[8])


def test_select_children_fail_noowner(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, _, ids = pack

    with pytest.raises(expected_exception=Error):
        clsr_select_children(cur, uuid4(), ids[8])


def test_select_children_fail_noid(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, _, _ = pack

    with pytest.raises(expected_exception=Error):
        clsr_select_children(cur, owner2, uuid4())


def test_select_descendants_fail_parent_owner(
    pack: tuple[Cursor, UUID, Sequence[UUID]]
):

    cur, owner, ids = pack

    assert clsr_select_descendants(cur, owner, ids[8]) == []

    with pytest.raises(expected_exception=Error):
        clsr_select_descendants(cur, owner2, ids[8])


def test_select_descendants_fail_noowner(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, _, ids = pack

    with pytest.raises(expected_exception=Error):
        clsr_select_descendants(cur, uuid4(), ids[8])


def test_select_descendants_fail_noid(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, _, _ = pack

    with pytest.raises(expected_exception=Error):
        clsr_select_descendants(cur, owner2, uuid4())


def test_delete_node_fail_noid(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    with pytest.raises(expected_exception=Error):
        clsr_delete_node(cur, owner, uuid4())

    with pytest.raises(expected_exception=Exception):
        clsr_delete_node(cur, owner2, ids[0])


def test_delete_descendants_fail_noid(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    with pytest.raises(expected_exception=Error):
        clsr_delete_descendants(cur, owner, uuid4())

    with pytest.raises(expected_exception=Error):
        clsr_delete_descendants(cur, owner2, ids[0])


# len
# owner nao existe -> raise --OKKKKKKKKK
# owner existe mas nao tem nodes ->return 0 -- OKKKKKKK

# insert
# owner nao existe ->raise --OKKKKKKKKKKKK
# parent nao existe para o owner dado ->raise ---OKKKKKK
# ja existe outro node com mesmo parent, node e node_type --OKKKKKKK

# select_byid
# id not found -> raise --OKKKKKKKK

# select (children, descendant)
# id nao existe para o owner-> raise --OKKKKK
# id existe mas nao children -> return [] --OKKKKK

# delete (node, descendants)
# id não existe -> raise --OKKKK
