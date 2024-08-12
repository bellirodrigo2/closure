CREATE OR REPLACE PROCEDURE table_exists(
    p_table_name text,
    p_schema text
)
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = p_schema
          AND table_name = quote_ident(p_table_name)
    ) THEN
        RAISE EXCEPTION 'Table "%" does not exist', p_table_name;
    END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE columns_exists(
    p_table_name text,
    p_columns text[], 
    p_schema text
)
LANGUAGE plpgsql
AS $$
DECLARE
    col text;
    column_exists boolean;
BEGIN

    CALL table_exists(p_table_name, p_schema);

    FOR col IN SELECT * FROM unnest(p_columns)
    LOOP
    EXECUTE FORMAT(
            'SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = %L
                  AND table_name = %L
                  AND column_name = %L
            )',
            p_schema,
            p_table_name,
            col
        ) INTO column_exists;

        IF NOT column_exists THEN
            RAISE EXCEPTION 'Column "%" does not exist in table "%"', col, p_table_name;
        END IF;
    END LOOP;
END;
$$;


CREATE OR REPLACE FUNCTION get_dynamic_select(
    p_table_name text,
    p_columns text[], 
    p_schema text DEFAULT 'public'
)
RETURNS TEXT AS
$$
DECLARE
    column_list text;
BEGIN

    CALL columns_exists(p_table_name, p_columns, p_schema);

    -- Construct the SQL query
    column_list := array_to_string(p_columns, ', ');
    RETURN 'SELECT ' || column_list || ' FROM ' || quote_ident(p_table_name);

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_dynamic_bucket_select(
    p_table_name text,
    p_columns text[],
    p_columns_select text[],
    p_schema text DEFAULT 'public'
)
RETURNS TEXT AS
$$
DECLARE
    column_list text;
BEGIN
    CALL columns_exists(p_table_name, p_columns, p_schema);

    -- Construct the SQL query
    column_list := array_to_string(p_columns_select, ', ');
    RETURN 'SELECT ' || column_list || ' FROM ' || quote_ident(p_table_name);

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION get_dynamic_select2(
    p_table_name text,
    p_columns text[],
    p_columns_select text[] DEFAULT NULL,
    p_schema text DEFAULT 'public'
)
RETURNS TEXT AS
$$
DECLARE
    column_list text;
    f_columns_select text[];
BEGIN
    CALL columns_exists(p_table_name, p_columns, p_schema);

    -- Construct the SQL query
    IF p_columns_select IS NULL THEN
        f_columns_select := p_columns;
    ELSE
        f_columns_select := p_columns_select;
    END IF;

    column_list := array_to_string(f_columns_select, ', ');
    RETURN 'SELECT ' || column_list || ' FROM ' || quote_ident(p_table_name);

END;
$$ LANGUAGE plpgsql;