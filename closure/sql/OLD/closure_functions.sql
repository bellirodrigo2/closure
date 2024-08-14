
CREATE OR REPLACE FUNCTION clsr_len(
    p_owner UUID
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_count INTEGER;
BEGIN
    -- IF OWNER DOES NOT EXIST, RAISE
    CALL owner_exist(p_owner);

    SELECT COUNT(*) INTO v_count FROM inode WHERE owner = p_owner;
    RETURN v_count;
END;
$$;

CREATE OR REPLACE PROCEDURE owner_exist(
    p_owner UUID
)
LANGUAGE plpgsql
AS $$
BEGIN
 IF NOT EXISTS (
        SELECT 1
        FROM users
        WHERE users.id = p_owner
    ) THEN
        RAISE EXCEPTION 'User %, does not exist', p_owner;
    END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE is_owner(
    p_id UUID,
    p_owner UUID
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_count INTEGER;
BEGIN

    -- CALL owner_exist(p_owner);
    IF p_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM inode n
        WHERE n.owner = p_owner
        AND n.id = p_id
    ) THEN
        RAISE EXCEPTION 'ID %, is not owned by %', p_id, p_owner;
    END IF;
END;
$$;


CREATE OR REPLACE PROCEDURE has_similar_root(
    p_name VARCHAR(64),
    p_node_type NODETYPE,
    p_owner UUID
)
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 
            FROM inode n
            JOIN link t ON (n.id = t.child)
            WHERE n.owner = p_owner
            AND n.name = p_name
            AND n.node_type = p_node_type
            GROUP BY t.child HAVING COUNT(t.child) = 1
    ) THEN
        RAISE EXCEPTION 'Root %s named % already exists for user %', p_node_type, p_name, p_owner;
    END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE has_similar_sibling(
    p_parent UUID,
    p_name VARCHAR(64),
    p_node_type NODETYPE,
    p_owner UUID
)
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM inode n
        JOIN link t ON n.id = t.child
        WHERE t.parent = p_parent
        AND t.depth = 1
        AND n.name = p_name
        AND n.node_type = p_node_type
        AND n.owner = p_owner
    ) THEN
        RAISE EXCEPTION 'Given parent node for user %, already has a % child named %', p_owner, p_node_type, p_name;
    END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE has_child(
    p_parent UUID,
    p_name VARCHAR(64),
    p_node_type NODETYPE,
    p_owner UUID
)
LANGUAGE plpgsql
AS $$
BEGIN

    IF p_parent IS NULL THEN
        CALL has_similar_root(p_name, p_node_type, p_owner);
    ELSE
        CALL has_similar_sibling(p_parent, p_name, p_node_type, p_owner);
    END IF;

END;
$$;

CREATE OR REPLACE PROCEDURE check_hierarchy(
    p_parent UUID, 
    p_node_type NODETYPE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_parent_type NODETYPE;
BEGIN

    SELECT n.node_type FROM inode n
    INTO v_parent_type
    WHERE  n.id = p_parent
    -- AND n.owner = p_owner -- AQUI È PARA JA FAZER O is_owner
    -- MAS SE ESSE SELECT retornar vazio, como fica o if abaixo com 'v_parent_type = NULL' 
    ;

    IF 
        p_node_type = 'node' AND (v_parent_type is NOT NULL OR v_parent_type <> 'node')
        OR
        p_node_type = 'item' AND (v_parent_type IS NULL OR v_parent_type = 'template')
        OR
        p_node_type = 'template' AND v_parent_type IS NOT NULL
     THEN
        RAISE EXCEPTION 'Node type % can´t have a parent %s', p_node_type,  v_parent_type;

    END IF;

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

CREATE OR REPLACE FUNCTION select_root_byid(
    p_id UUID,
    p_owner UUID
)
RETURNS TABLE (id UUID, name VARCHAR(64), template UUID, node_type NODETYPE)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT n.id, n.name, n.template, n.node_type FROM inode n
        WHERE n.id IN (
            SELECT parent FROM link t
            WHERE t.child = p_id
            AND n.owner = p_owner
            ORDER BY t.depth DESC
            LIMIT 1
        );
END;
$$;

CREATE OR REPLACE FUNCTION select_roots(
    p_owner UUID
)
RETURNS TABLE (id UUID, name VARCHAR(64), template UUID, node_type NODETYPE)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT n.id, n.name, n.template, n.node_type FROM inode n
        JOIN link t ON (n.id = t.child)
        WHERE n.owner = p_owner 
        GROUP BY n.id, n.name, n.template, n.node_type
        HAVING COUNT(t.child) = 1;
END;
$$;

CREATE OR REPLACE FUNCTION select_byid(
    p_id UUID,
    p_owner UUID
)
RETURNS TABLE (id UUID, name VARCHAR(64), template UUID, node_type NODETYPE)
LANGUAGE plpgsql
AS $$
BEGIN

    RETURN QUERY
    SELECT n.id, n.name, n.template, n.node_type
        FROM inode n
        WHERE n.id = p_id AND n.owner = p_owner;
END;
$$;

CREATE OR REPLACE FUNCTION select_child(
    p_parent_id UUID,
    p_owner UUID
)
RETURNS TABLE (id UUID, name VARCHAR(64), template UUID, node_type NODETYPE)
LANGUAGE plpgsql
AS $$
BEGIN

    CALL is_owner(p_parent_id, p_owner);

    RETURN QUERY
    SELECT t.child, n.name, n.template, n.node_type FROM inode n
            JOIN link t ON (n.id = t.child) 
            WHERE t.parent = p_parent_id 
            AND t.depth = 1
            AND n.owner = p_owner;
END;
$$;

CREATE OR REPLACE FUNCTION select_descendants(
    p_parent_id UUID,
    p_owner UUID
)
RETURNS TABLE (parent_id UUID, id UUID, name VARCHAR(64), template UUID, node_type NODETYPE)
LANGUAGE plpgsql
AS $$
BEGIN

    CALL is_owner(p_parent_id, p_owner);

    RETURN QUERY
    SELECT t.parent, n.id, n.name, n.template, n.node_type FROM inode n
             JOIN link t ON (n.id = t.child) 
             WHERE t.parent = p_parent_id 
             AND t.depth > 0
             AND n.owner = p_owner
             ORDER BY t.depth ASC;
END;
$$;


CREATE OR REPLACE FUNCTION delete_node(
    p_id UUID,
    p_owner UUID
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted_count INTEGER;
BEGIN
    
    -- IF parent does not has same owner, raises
    CALL is_owner(p_id, p_owner);

    UPDATE link SET depth = depth -1 WHERE child IN
        (
            SELECT child FROM link
            WHERE parent = p_id 
            AND depth >= 1
            -- AND owner = p_owner
        )
        AND depth >= 1;
    DELETE FROM inode WHERE id = p_id; -- AND owner = p_owner;

        -- Get the number of rows deleted
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;

    -- Return the number of deleted rows
    RETURN v_deleted_count;
END;
$$;

CREATE OR REPLACE FUNCTION delete_descendants(
    p_id UUID,
    p_owner UUID
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted_count INTEGER;
BEGIN
    
    -- IF parent does not has same owner, raises
    CALL is_owner(p_id, p_owner);


    DELETE FROM inode n WHERE n.id IN
        (
        SELECT DISTINCT t.child FROM link t
            WHERE t.parent = p_id
        );
        -- Get the number of rows deleted
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;

    -- Return the number of deleted rows
    RETURN v_deleted_count;
END;
$$;