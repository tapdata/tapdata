SELECT
    t.relname AS table_name,
    i.relname AS index_name,
    a.attname AS column_name,
    ix.indisunique AS is_unique,
    ix.indisprimary AS is_primary,
    (CASE WHEN ix.indoption[a.attnum-1]&1=0 THEN 'A' ELSE 'D' END) AS asc_or_desc
FROM
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_attribute a,
    information_schema.tables tt
WHERE
        t.oid = ix.indrelid
  AND i.oid = ix.indexrelid
  AND a.attrelid = t.oid
  AND a.attnum = ANY(ix.indkey)
  AND t.relkind = 'r'
  AND tt.table_name=t.relname
  AND tt.table_catalog='%s'
  AND tt.table_schema='%s'
    %s
ORDER BY t.relname, i.relname, a.attnum