SELECT col.*, d.description,
       (SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) AS "dataType"
        FROM pg_catalog.pg_attribute a
        WHERE a.attnum > 0
          AND a.attname = col.column_name
          AND NOT a.attisdropped
          AND a.attrelid =
              (SELECT max(cl.oid)
               FROM pg_catalog.pg_class cl
                        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = cl.relnamespace
               WHERE cl.relname = col.table_name))
FROM information_schema.columns col
         JOIN pg_class c ON c.relname = col.table_name
         LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = col.ordinal_position
WHERE col.table_catalog='%s' AND col.table_schema='%s' %s
ORDER BY col.table_name,col.ordinal_position