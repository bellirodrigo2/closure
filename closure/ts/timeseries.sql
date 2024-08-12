CREATE TYPE STATUSTYPE AS ENUM ('ok', 'null', 'scan_off', 'bad_val');

CREATE TABLE data_point (
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE --14PT103(MEAS)
);

CREATE TABLE data_point_meta(
    data_point uuid REFERENCES data_point(id) ON DELETE CASCADE,
    data_type DATATYPE NOT NULL, --FLOAT8
    descriptor VARCHAR(255)
    -- metadata HSTORE ????
);

CREATE TABLE data_point_addr(
    data_point UUID UNIQUE NOT NULL,
    table_name VARCHAR NOT NULL,              -- digester.feed
    time_col VARCHAR NOT NULL,                -- time         
    value_col VARCHAR NOT NULL,               -- price        
    status_col VARCHAR,                       -- NULL 
    args HSTORE,           NAO PODE SER ONE TO MANY PQ VALUE PODE  TER DIFERENTES TYPES                   -- 'symbol => "APP"'
    FOREIGN KEY (data_point) REFERENCES data_point(id) ON DELETE CASCADE -- ONE TO ONE RELATIONSHIP
);

CREATE TABLE timed_table (time TIMESTAMP);
CREATE TABLE timed_table_tz (time TIMESTAMPTZ);