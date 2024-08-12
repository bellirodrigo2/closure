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
