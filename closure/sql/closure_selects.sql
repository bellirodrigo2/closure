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
RETURNS TABLE (child UUID, name VARCHAR(64))
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
        SELECT t.child, n.name
        FROM inode n
        JOIN link t ON n.id = t.child
        WHERE t.parent = p_parent_id
        AND t.depth = 1
        AND n.owner = p_owner;
END;
$$;

-- Function to return a detailed result set
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
    WITH child_paths AS (
        SELECT * FROM select_internal_child_path(p_parent_id, p_owner)
    )
    SELECT DISTINCT n.id, n.name, 
        string_agg(n.name, '.') OVER (PARTITION BY t.child ORDER BY t.depth DESC) AS path, 
        n.template, n.node_type
    FROM inode n
    JOIN link t ON t.parent = n.id
    -- INNER JOIN child_paths cp
    -- ON cp.child = n.id
    WHERE t.child IN (SELECT cp.child FROM child_paths cp)
    ;
END;
$$;


