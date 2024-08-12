from collections.abc import Iterable
from enum import Enum


class DataType(Enum): ...

#TODO 
#tem que ter um tipo de mixtable com mais de uma coluna de valor...
# tipo 
    # time
    # stock_symbol
    # stock_price
    # volume

def create_table(
    schema: str,
    table_name: str,
    tz: bool,
    **key_cols: tuple[str, DataType]
): ...

def add_column(
    schema: str,
    table_name: str,
    value_col: tuple[str, DataType],
    status_col: tuple[str, DataType]
): ...

#if value_col exists, status_col should exist, and label of filters should exists... 
    #in this case it does not add any column to the sql table itself
def add_datapoint(
    schema: str,
    table_name: str,
    value_col: tuple[str, DataType],
    status_col: tuple[str, DataType],
    **filters: tuple[str, str]
): ...

def search_tag(word_fnmatch: str, data_type: DataType | None = None):
    "fnmatch word style tag search, with optional data_type"
    ...


def build_query_single(table_name:str, time_col:str| None, **cols: tuple[str | None, str | None]) -> str: 
    "cols is label: value col or NULL, status col or NULL, if both null ignore"
    ...


def build_query_mix(table_name:str, time_col:str| None, value_col:str | None, status_col:str | None, **args:str) -> str: ...
    
CREATE TABLE data_point_map (
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE, --14PT103(MEAS)
    data_type DATATYPE NOT NULL, --FLOAT8
    metadata HSTORE
);

CREATE TABLE data_addr(
    data_point UUID UNIQUE NOT NULL,
    table_name VARCHAR NOT NULL,              -- digester.feed ||  digester.vessel
    time_col VARCHAR NOT NULL,                -- time          ||  time
    value_col VARCHAR NOT NULL,               -- PT103         ||  value
    status_col VARCHAR,                       -- PT103_status  ||  NULL
    args HSTORE,
    FOREIGN KEY (data_point) REFERENCES data_point_map(id) ON DELETE CASCADE -- ONE TO ONE RELATIONSHIP
);