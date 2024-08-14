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

CREATE OR REPLACE FUNCTION select_children_json(
    p_parent_id UUID,
    p_owner UUID
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    result jsonb;
BEGIN

    CALL is_owner(p_parent_id, p_owner);

    SELECT jsonb_agg(
        jsonb_build_object(
            'id', n.id,
            'name', n.name,
            'template', n.template,
            'node_type', n.node_type
        )
    ) INTO result
    FROM inode n
    JOIN link t ON (n.id = t.child) 
    WHERE t.parent = p_parent_id 
    AND t.depth = 1
    AND n.owner = p_owner;

    RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION select_descendants_json(
    p_parent_id UUID,
    p_owner UUID
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    result jsonb;
BEGIN

    CALL is_owner(p_parent_id, p_owner);

    SELECT jsonb_agg(
        jsonb_build_object(
            'parent_id', t.parent,
            'id', n.id,
            'name', n.name,
            'template', n.template,
            'node_type', n.node_type
        )
    ) INTO result
    FROM inode n
    JOIN link t ON (n.id = t.child)
    WHERE t.parent = p_parent_id
    AND t.depth > 0
    AND n.owner = p_owner;
    RETURN result;
END;
$$;

-- Function to return a set of UUIDs
CREATE OR REPLACE FUNCTION select_internal_child_path(
    p_parent_id UUID,
    p_owner UUID
)
RETURNS TABLE (child UUID)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
        SELECT t.child
        FROM inode n
        JOIN link t ON n.id = t.child
        WHERE t.parent = p_parent_id
        AND t.depth = 1
        AND n.owner = p_owner;
END;
$$;

CREATE OR REPLACE FUNCTION select_child_path(
    p_parent_id UUID,
    p_owner UUID
)
RETURNS TABLE (id UUID, name VARCHAR(64), path TEXT, template UUID, node_type NODETYPE)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Call another function for permission check or similar
    CALL is_owner(p_parent_id, p_owner);

    -- Use subquery with function call correctly
    RETURN QUERY
    WITH child_ids AS (
            SELECT child FROM select_internal_child_path(p_parent_id, p_owner)
        ), child_ids_names AS (
        SELECT n.name, t.child
        FROM inode n
        JOIN link t ON t.parent = n.id
        WHERE t.child IN (SELECT child FROM child_ids)
        ORDER BY t.depth DESC
        ), paths AS (
        SELECT c.child, string_agg(c.name, '.') AS path FROM child_ids_names c 
        GROUP BY c.child)
        SELECT n.id, n.name, p.path, n.template, n.node_type FROM paths p
        JOIN inode n ON p.child = n.id;
END;
$$;


CREATE OR REPLACE FUNCTION select_descendants_path(
    p_parent_id UUID,
    p_owner UUID
)
RETURNS TABLE (parent_id UUID, id UUID, name VARCHAR(64), path TEXT,template UUID, node_type NODETYPE)

LANGUAGE plpgsql
AS $$
BEGIN
    -- Call another function for permission check or similar
    CALL is_owner(p_parent_id, p_owner);

    -- Use subquery with function call correctly
    RETURN QUERY
    WITH child_ids AS (
        SELECT child FROM (
           SELECT t.child
            FROM inode n
            JOIN link t ON n.id = t.child
            WHERE t.parent = p_parent_id
            AND t.depth > 0
            AND n.owner = p_owner
        )
    ), child_ids_names AS (
        SELECT n.name, t.child
        FROM inode n
        JOIN link t ON t.parent = n.id
        WHERE t.child IN (SELECT child FROM child_ids)
        ORDER BY t.child, t.depth DESC
    ), paths AS (
        SELECT 
        c.child, string_agg(c.name, '.') AS path FROM child_ids_names c 
        GROUP BY c.child
    )
    SELECT l.parent, n.id, n.name, p.path, n.template, n.node_type FROM paths p
        JOIN inode n ON p.child = n.id
        JOIN link l ON p.child = l.child
        WHERE l.depth = 1
        ;
END;
$$;



CREATE OR REPLACE FUNCTION select_child_byname(
    p_parent_id UUID,
    p_owner UUID,
    p_name VARCHAR
)
RETURNS TABLE (id UUID, name_ VARCHAR(64), template UUID, node_type NODETYPE)
LANGUAGE plpgsql
AS $$
BEGIN

    CALL is_owner(p_parent_id, p_owner);

    RETURN QUERY
    SELECT * 
        FROM select_child(p_parent_id, p_owner) 
        WHERE name = p_name;
END;
$$;

CREATE OR REPLACE FUNCTION select_child_bypath(
    p_owner UUID,
    p_root TEXT,
    p_names TEXT[]
)
RETURNS TABLE (id_ UUID, name_ VARCHAR(64), template_ UUID, node_type_ NODETYPE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_string TEXT;
    v_id UUID;
    v_old UUID;
BEGIN

    SELECT id 
        FROM select_roots(p_owner)
        WHERE name = p_root
        INTO v_id;
    v_old := v_id;
    FOREACH v_string IN ARRAY p_names
    LOOP
        SELECT id 
            FROM select_child(v_id, p_owner) 
            WHERE name = v_string
            INTO v_id;
            
        IF v_id IS NULL THEN
            RAISE EXCEPTION 'Error on select by path. Parent %, Name %s', v_old, v_string;
        END IF;
        v_old := v_id;
    END LOOP;

    RETURN QUERY
    SELECT id, name, template, node_type 
    FROM inode
    WHERE id = v_id;
END;
$$;