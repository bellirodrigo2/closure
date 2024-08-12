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