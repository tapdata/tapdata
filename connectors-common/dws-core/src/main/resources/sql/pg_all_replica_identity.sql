SELECT tab.tablename, cls.relreplident,
       (CASE WHEN EXISTS
           (SELECT 1 FROM pg_index WHERE indrelid IN
                                         (SELECT oid FROM pg_class WHERE relname = tab.tablename))
                 THEN 1 ELSE 0 END) hasunique
FROM pg_tables tab
         JOIN pg_class cls ON cls.relname = tab.tablename
WHERE tab.schemaname = '%s' %s