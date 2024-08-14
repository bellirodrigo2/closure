from enum import Enum
from typing import Any
from uuid import UUID

from psycopg import Cursor
from pydantic import BaseModel


class Template(BaseModel):

    id: UUID | None
    name: str
    owner: UUID


class DataType(Enum): ...


class DataSource(Enum): ...


class TemplateItem(BaseModel):

    id: UUID | None
    name: str
    template: UUID
    parent: UUID | None
    data_type: DataType
    data_source: DataSource
    data_config: str | None


def tmplt_insert_template(cur: Cursor, owner: UUID, template: Template) -> Any:
    cur.execute(
        """
                SELECT template.add_template(%s, %s)
                """,
        (template.name, template.owner),
    )
    return cur.fetchone()


def tmplt_insert_item(
    cur: Cursor, owner: UUID, parent: UUID | None, template: TemplateItem
) -> Any: ...
