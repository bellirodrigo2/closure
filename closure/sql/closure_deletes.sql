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
    
    CALL is_owner(p_id, p_owner);
-- Create a temporary table to store child nodes and their depths
    CREATE TEMP TABLE temp_rows (
        tgt UUID,
        dep INTEGER
    );

    -- Populate the temporary table with child nodes and their depths
    INSERT INTO temp_rows (tgt, dep)
    SELECT t.child AS tgt, t.depth AS dep
    FROM link t
    WHERE t.parent = p_id
      AND t.depth > 0;

    -- Delete the node from the inode table
    DELETE FROM inode WHERE id = p_id;

    -- Get the count of rows affected by the DELETE statement
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;

    -- Update the depth of child nodes based on the temporary table
    UPDATE link
    SET depth = depth - 1
    WHERE child IN (
        SELECT tgt FROM temp_rows
    )
    AND depth > (
        SELECT dep FROM temp_rows WHERE tgt = link.child
    )
    ;

    -- Drop the temporary table
    DROP TABLE temp_rows;

    RETURN v_deleted_count;
END;
$$;