CREATE SCHEMA template;

CREATE TABLE template.node (
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    owner uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_name_owner UNIQUE (name, owner)
);

CREATE TABLE template.item (
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,

    template uuid NOT NULL REFERENCES template.node(id) ON DELETE CASCADE,
    parent uuid REFERENCES template.item(id) ON DELETE CASCADE,

    name VARCHAR(64) NOT NULL,
    data_type DATATYPE,
    data_source DATASOURCE NOT NULL, --datapoint, calc, agg, etc
    data_config TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER set_updated_at
BEFORE UPDATE ON template.node
FOR EACH ROW
EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER set_updated_at
BEFORE UPDATE ON template.item
FOR EACH ROW
EXECUTE FUNCTION update_updated_at();

CREATE OR REPLACE FUNCTION template.add_template(
    p_name VARCHAR(64), 
    p_owner UUID
)
RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
    v_new_inode_id UUID;
BEGIN
    INSERT INTO template.node (name, owner) 
    VALUES (p_name, p_owner) 
    RETURNING id into v_new_inode_id;

    RETURN v_new_inode_id;
END;
$$;

-- ADD ITEM

-- UPDATE TEMPLATE
-- UPDATE ITEM

-- GET CHILDREN TEMPLATE
-- GET CHILDREN ITEM

-- GET DESCENDANTS TEMPLATE
-- GET DESCENDANTS ITEM

--DELETE TEMPLATE
--DELETE ITEM