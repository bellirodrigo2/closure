CREATE OR REPLACE FUNCTION insert_plain_node(
    p_name VARCHAR(64), 
    p_owner UUID,
    p_node_type NODETYPE,
    p_parent UUID
)
RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
    v_inode_id UUID;
BEGIN
    -- insert node 
    INSERT INTO inode (name, node_type, owner, template) 
    VALUES (p_name, p_node_type, p_owner, NULL)
    RETURNING id INTO v_inode_id;

    -- insert links
    CALL insert_link(p_parent, v_inode_id);

    RETURN v_inode_id;

EXCEPTION
    WHEN others THEN
        RAISE NOTICE 'Error inserting into inode or link table: %', SQLERRM;
        RAISE;
END;
$$;

CREATE OR REPLACE FUNCTION insert_node(
    p_name VARCHAR(64), 
    p_owner UUID,
    p_node_type NODETYPE,
    p_template UUID DEFAULT NULL,
    p_parent UUID DEFAULT NULL
)
RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
    v_new_inode_id UUID;
BEGIN

    -- IF parent does not has same owner, raises
    CALL is_owner(p_parent, p_owner);

    -- CALL check_hierarchy(p_parent, p_node_type);

    -- If there is a child with same name and type, raises
    CALL has_child(p_parent, p_name, p_node_type, p_owner);

    --is_onwer return parent type ???
    --FAZER UMA FUNÇÃO ESPECIFICA... is_owner, has_child e return parent_type

    IF p_template IS NULL THEN
        v_new_inode_id := insert_plain_node(p_name, p_owner, p_node_type, p_parent);
    ELSE
        RAISE NOTICE 'Template is not NULL. Template value: %', p_template;
    END IF;
    RETURN v_new_inode_id;
END;
$$;

CREATE OR REPLACE PROCEDURE insert_link(
    p_parent UUID,
    p_child UUID
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Insert the node itself
    INSERT INTO link (parent, child, depth)
    VALUES (p_child, p_child, 0);

    --  if parent, insert all other links
    IF p_parent IS NOT NULL THEN
        INSERT INTO link (parent, child, depth)
        SELECT link.parent, p_child, row_number() OVER (ORDER BY link.depth) 
        FROM link
        WHERE link.child = p_parent;
    END IF;

EXCEPTION
    WHEN unique_violation THEN
        -- Handle unique constraint violation (i.e., (parent, child) already exists)
        RAISE NOTICE 'Conflict: (parent, child) pair (%-%): %', p_parent, p_child, SQLERRM;
        RAISE;  -- Re-raise the exception to propagate it
    WHEN others THEN
        -- Handle other exceptions
        RAISE NOTICE 'Error inserting into link table: %', SQLERRM;
        RAISE;  -- Re-raise the exception to propagate it
END;
$$;
