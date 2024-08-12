CREATE TABLE users (
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    name VARCHAR(64) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TYPE DATATYPE AS ENUM ('float32', 'float64', 'byte','integer32', 'uinteger32', 'uinteger64', 'integer64', 'boolean', 'string');
CREATE TYPE DATASOURCE AS ENUM ('datapoint', 'formula', 'calc', 'aggregator','fixed');
CREATE TYPE NODETYPE AS ENUM ('node', 'item', 'template');


CREATE TABLE inode (
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    node_type NODETYPE NOT NULL,
    owner uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    template uuid REFERENCES inode(id) ON DELETE SET NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_inode_owner ON inode (owner);


CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_updated_at
BEFORE UPDATE ON inode
FOR EACH ROW
EXECUTE FUNCTION update_updated_at();

CREATE TABLE item (
    data_type DATATYPE,
    data_source DATASOURCE,
    config_str VARCHAR,
    owner uuid NOT NULL REFERENCES inode(id) ON DELETE CASCADE
);

CREATE TABLE link(
    parent uuid NOT NULL REFERENCES inode(id) ON DELETE CASCADE,
    child uuid NOT NULL REFERENCES inode(id) ON DELETE CASCADE,
    depth INTEGER NOT NULL,
    PRIMARY KEY (parent, child)
);