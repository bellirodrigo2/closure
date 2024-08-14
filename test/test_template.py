from uuid import UUID

import pytest
from psycopg import Cursor, Error

from closure.db import bootstrap
from closure.template import (
    Template,
    TemplateItem,
    tmplt_insert_item,
    tmplt_insert_template,
)

template_name = "template"


@pytest.fixture(scope="function")  # , params=[None, "USER"])
def pack_template():  # request: pytest.FixtureRequest):

    with bootstrap() as conn:

        cur = conn.cursor()

        owner = UUID("3c07ee61-4fc0-44ca-b2ad-e6820f614f74")

        template = Template(id=None, name=template_name, owner=owner)
        tmplt_insert_template(cur, owner, template)
        conn.commit()

        yield cur, owner

        # TODO preciso entender pq precisa desse commit???
        conn.commit()
        try:
            cur.execute("DELETE FROM template.node")  # Cleanup the test data
            conn.commit()  # Ensure cleanup changes are committed
        except Exception:
            print("Error on Template Test Clean Up")
            conn.rollback()  # Rollback if cleanup fails (optional)
        finally:
            cur.close()  # Close cursor
            conn.close()  # Close connection


def test_insert_template(pack_template: tuple[Cursor, UUID]):

    cur, owner = pack_template

    temp1 = Template(id=None, name="temp1", owner=owner)
    (inserted1,) = tmplt_insert_template(cur, owner, temp1)
    assert isinstance(inserted1, UUID) == True

    temp2 = Template(id=None, name="temp2", owner=owner)
    (inserted2,) = tmplt_insert_template(cur, owner, temp2)
    assert isinstance(inserted2, UUID) == True

    with pytest.raises(expected_exception=Error):
        tmplt_insert_template(cur, owner, temp1)


def test_insert_item(pack_template: tuple[Cursor, UUID]):

    cur, owner = pack_template

    item = TemplateItem()
    tmplt_insert_item(cur, owner, None, item)


# TODO pensar no frontend
# TODO select_descendants com next steps...precisa saber total level
# OU TIPO START END
# TODO recriar a tree in memory a partir do descendants.... ou descendants com next steps
