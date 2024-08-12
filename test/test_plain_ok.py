from collections.abc import Sequence
from copy import deepcopy
from typing import Any
from uuid import UUID

import pytest
from psycopg import Cursor

from closure.closure import (
    Inode,
    clsr_delete_descendants,
    clsr_delete_node,
    clsr_get_path,
    clsr_insert,
    clsr_len,
    clsr_select_byid,
    clsr_select_children,
    clsr_select_children_json,
    clsr_select_descendants,
    clsr_select_descendants_json,
    clsr_select_root_byid,
    clsr_select_roots,
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
    (id9,) = clsr_insert(cur, owner, id2, nodes[9])
    (id10,) = clsr_insert(cur, owner, id4, nodes[10])
    (id11,) = clsr_insert(cur, owner, id6, nodes[11])
    (id12,) = clsr_insert(cur, owner, id8, nodes[12])
    (id13,) = clsr_insert(cur, owner, id8, nodes[13])
    (id14,) = clsr_insert(cur, owner, id8, nodes[14])
    (id15,) = clsr_insert(cur, owner, id4, nodes[15])
    (id16,) = clsr_insert(cur, owner, id10, nodes[16])
    (id17,) = clsr_insert(cur, owner, id10, nodes[17])
    (id18,) = clsr_insert(cur, owner, id16, nodes[18])
    (id19,) = clsr_insert(cur, owner, id16, nodes[19])

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
        id9,
        id10,
        id11,
        id12,
        id13,
        id14,
        id15,
        id16,
        id17,
        id18,
        id19,
    ]


n = 20


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


# cur.execute("""SELECT t.parent, n.name, t.depth FROM link t
# JOIN inode n ON n.id = t.child
# ;""")
# rows = cur.fetchall()
# for row in rows:
# print(row)


def get_name(rows: list[Any], idx: int | str):
    return sorted([i[idx] for i in rows])


def test_len(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, _ = pack

    (len_,) = clsr_len(cur, owner)

    assert len_ == n


def test_root(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    roots = clsr_select_roots(cur, owner)
    assert get_name([x[0] for x in roots], 1) == sorted(["NODE0"])

    root = clsr_select_root_byid(cur, owner, ids[0])
    assert root[0][1] == "NODE0"  # type: ignore

    root2 = clsr_select_root_byid(cur, owner, ids[1])
    assert root2[0][1] == "NODE0"  # type: ignore

    root3 = clsr_select_root_byid(cur, owner, ids[18])
    assert root3[0][1] == "NODE0"  # type: ignore


def test_select_byid(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack
    node4 = clsr_select_byid(cur, owner, ids[4])
    assert (node4[1]) == "NODE4"  # type: ignore

    node13 = clsr_select_byid(cur, owner, ids[13])
    assert (node13[1]) == "NODE13"  # type: ignore

    node0 = clsr_select_byid(cur, owner, ids[0])
    assert (node0[1]) == "NODE0"  # type: ignore


def test_select_child(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    children = [i[0] for i in clsr_select_children(cur, owner, ids[0])]
    assert get_name(children, 1) == sorted(["NODE1", "NODE2"])


def test_select_descendants(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    descendants = [i[0] for i in clsr_select_descendants(cur, owner, ids[0])]
    assert get_name(descendants, 2) == sorted([x.name for x in nodes[1:]])

    descendants2 = [i[0] for i in clsr_select_descendants(cur, owner, ids[16])]
    assert get_name(descendants2, 2) == sorted(["NODE18", "NODE19"])


def test_delete_node(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    tgt = ids[10]

    children = [i[0] for i in clsr_select_children(cur, owner, tgt)]
    assert get_name(children, 1) == sorted(["NODE16", "NODE17"])

    descendants = [i[0] for i in clsr_select_descendants(cur, owner, tgt)]
    assert get_name(descendants, 2) == sorted(["NODE16", "NODE17", "NODE18", "NODE19"])

    nums = clsr_delete_node(cur, owner, ids[16])
    assert nums[0] == 1

    children2 = [i[0] for i in clsr_select_children(cur, owner, tgt)]
    assert get_name(children2, 1) == sorted(["NODE17", "NODE18", "NODE19"])
    descendants2 = [i[0] for i in clsr_select_descendants(cur, owner, tgt)]
    assert get_name(descendants2, 2) == sorted(["NODE17", "NODE18", "NODE19"])


def test_delete_node2(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    tgt = ids[1]

    children = [i[0] for i in clsr_select_children(cur, owner, tgt)]
    assert get_name(children, 1) == sorted(["NODE3", "NODE4", "NODE5"])

    descendants = [i[0] for i in clsr_select_descendants(cur, owner, tgt)]
    desc = sorted(
        [
            "NODE3",
            "NODE4",
            "NODE5",
            "NODE10",
            "NODE15",
            "NODE16",
            "NODE17",
            "NODE18",
            "NODE19",
        ]
    )
    assert get_name(descendants, 2) == desc

    nums = clsr_delete_node(cur, owner, ids[4])
    assert nums[0] == 1

    children2 = [i[0] for i in clsr_select_children(cur, owner, tgt)]
    assert get_name(children2, 1) == sorted(["NODE3", "NODE5", "NODE10", "NODE15"])
    descendants2 = [i[0] for i in clsr_select_descendants(cur, owner, tgt)]
    popped_desc = deepcopy(desc)
    popped_desc.remove("NODE4")
    assert get_name(descendants2, 2) == popped_desc


def test_delete_descendants(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack
    tgt = ids[1]

    children = [i[0] for i in clsr_select_children(cur, owner, tgt)]
    assert get_name(children, 1) == sorted(["NODE3", "NODE4", "NODE5"])

    descendants = [i[0] for i in clsr_select_descendants(cur, owner, tgt)]
    desc = sorted(
        [
            "NODE3",
            "NODE4",
            "NODE5",
            "NODE10",
            "NODE15",
            "NODE16",
            "NODE17",
            "NODE18",
            "NODE19",
        ]
    )
    assert get_name(descendants, 2) == desc

    num = clsr_delete_descendants(cur, owner, ids[4])
    assert num[0] == 7

    children2 = [i[0] for i in clsr_select_children(cur, owner, tgt)]
    assert get_name(children2, 1) == sorted(["NODE3", "NODE5"])

    descendants2 = [i[0] for i in clsr_select_descendants(cur, owner, tgt)]
    assert get_name(descendants2, 2) == sorted(["NODE3", "NODE5"])


def test_delete_descendants2(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    num = clsr_delete_descendants(cur, owner, ids[4])
    assert num[0] == 7

    num2 = clsr_delete_descendants(cur, owner, ids[2])
    assert num2[0] == 9

    (len_,) = clsr_len(cur, owner)
    assert len_ == 4

    descendants2 = [i[0] for i in clsr_select_descendants(cur, owner, ids[0])]
    assert get_name(descendants2, 2) == sorted(["NODE1", "NODE3", "NODE5"])


def test_insert_second_tree(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    nodes2 = [make_node(i, "NAME") for i in range(n)]
    ids2 = populate_tree(cur, nodes2, owner)

    (len_,) = clsr_len(cur, owner)

    assert len_ == n * 2
    node4 = clsr_select_byid(cur, owner, ids[4])
    assert (node4[1]) == "NODE4"  # type: ignore

    node4 = clsr_select_byid(cur, owner, ids2[4])
    assert (node4[1]) == "NAME4"  # type: ignore

    children = [i[0] for i in clsr_select_children(cur, owner, ids[0])]
    assert get_name(children, 1) == sorted(["NODE1", "NODE2"])

    descendants = [i[0] for i in clsr_select_descendants(cur, owner, ids[0])]
    assert get_name(descendants, 2) == sorted([x.name for x in nodes[1:]])

    children2 = [i[0] for i in clsr_select_children(cur, owner, ids2[0])]
    assert get_name(children2, 1) == sorted(["NAME1", "NAME2"])

    descendants2 = [i[0] for i in clsr_select_descendants(cur, owner, ids2[0])]
    assert get_name(descendants2, 2) == sorted([x.name for x in nodes2[1:]])

    tgt = ids[1]

    children = [i[0] for i in clsr_select_children(cur, owner, tgt)]
    assert get_name(children, 1) == sorted(["NODE3", "NODE4", "NODE5"])

    descendants = [i[0] for i in clsr_select_descendants(cur, owner, tgt)]
    desc = sorted(
        [
            "NODE3",
            "NODE4",
            "NODE5",
            "NODE10",
            "NODE15",
            "NODE16",
            "NODE17",
            "NODE18",
            "NODE19",
        ]
    )
    assert get_name(descendants, 2) == desc

    nums = clsr_delete_node(cur, owner, ids[4])
    assert nums[0] == 1

    children2 = [i[0] for i in clsr_select_children(cur, owner, tgt)]
    assert get_name(children2, 1) == sorted(["NODE3", "NODE5", "NODE10", "NODE15"])

    descendants2 = [i[0] for i in clsr_select_descendants(cur, owner, tgt)]
    popped_desc = deepcopy(desc)
    popped_desc.remove("NODE4")
    assert get_name(descendants2, 2) == popped_desc

    tgt2 = ids2[1]

    children2 = [i[0] for i in clsr_select_children(cur, owner, tgt2)]
    assert get_name(children2, 1) == sorted(["NAME3", "NAME4", "NAME5"])

    descendants2 = [i[0] for i in clsr_select_descendants(cur, owner, tgt2)]
    desc2 = sorted(
        [
            "NAME3",
            "NAME4",
            "NAME5",
            "NAME10",
            "NAME15",
            "NAME16",
            "NAME17",
            "NAME18",
            "NAME19",
        ]
    )
    assert get_name(descendants2, 2) == desc2

    nums2 = clsr_delete_node(cur, owner, ids2[4])
    assert nums2[0] == 1

    children2 = [i[0] for i in clsr_select_children(cur, owner, tgt2)]
    assert get_name(children2, 1) == sorted(["NAME3", "NAME5", "NAME10", "NAME15"])

    descendants2 = [i[0] for i in clsr_select_descendants(cur, owner, tgt2)]
    popped = deepcopy(desc2)
    popped.remove("NAME4")
    assert get_name(descendants2, 2) == popped


def test_insert_second_tree2(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    nodes2 = [make_node(i, "NAME") for i in range(n)]
    populate_tree(cur, nodes2, owner)

    (len_,) = clsr_len(cur, owner)

    assert len_ == n * 2

    (num,) = clsr_delete_descendants(cur, owner, ids[0])
    assert num == n

    (len_,) = clsr_len(cur, owner)
    assert len_ == n


def test_insert_second_tree_roots(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    nodes2 = [make_node(i, "NAME") for i in range(n)]
    ids2 = populate_tree(cur, nodes2, owner)

    roots = clsr_select_roots(cur, owner)
    assert get_name([x[0] for x in roots], 1) == sorted(["NODE0", "NAME0"])

    root1 = clsr_select_root_byid(cur, owner, ids[0])
    assert root1[0][1] == "NODE0"  # type: ignore

    root2 = clsr_select_root_byid(cur, owner, ids[8])
    assert root2[0][1] == "NODE0"  # type: ignore

    root3 = clsr_select_root_byid(cur, owner, ids2[0])
    assert root3[0][1] == "NAME0"  # type: ignore

    root4 = clsr_select_root_byid(cur, owner, ids2[8])
    assert root4[0][1] == "NAME0"  # type: ignore


def test_get_path(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    path = clsr_get_path(cur, owner, ids[6])

    assert path[0] == "NODE0.NODE2.NODE6"


def make_list_(st: int, end: int):
    return [f"NODE{i}" for i in range(st, end)]


def test_select_children_json(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    tgts = sorted(make_list_(6, 10))

    (children,) = clsr_select_children_json(cur, owner, ids[2])
    assert get_name(children, "name") == tgts


def test_select_descendants_json(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    tgts = sorted(make_list_(11, 15) + make_list_(6, 10))

    (descendants,) = clsr_select_descendants_json(cur, owner, ids[2])
    assert get_name(descendants, "name") == tgts


def test_children_w_path(pack: tuple[Cursor, UUID, Sequence[UUID]]):

    cur, owner, ids = pack

    print("\n")

    cur.execute(
        """
            SELECT select_child_path(%s, %s)""",
        (ids[2], owner),
    )
    rows = cur.fetchall()
    for r in rows:
        print(r)

    cur.execute(
        """
            SELECT select_internal_child_path(%s, %s)""",
        (ids[2], owner),
    )
    rows = cur.fetchall()
    for r in rows:
        print(r)

    # (children,) = clsr_select_children_json(cur, owner, ids[2])
    # for r in children:
    #     print(r)
