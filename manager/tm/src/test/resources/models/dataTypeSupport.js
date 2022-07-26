/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/11 下午3:52
 */

let dataTypes = [
    // source for oracle
    {sourceDbType: 'oracle', targetDbType: ['oracle', 'mongodb', 'sqlserver', 'mysql', 'postgres', 'elasticsearch', 'kafka', 'clickhouse'], operator: 'ignoreCaseIn', expression: ['RAW', 'LONG_RAW', 'BFILE', 'XMLTYPE', 'STRUCT'], support: false},
    {sourceDbType: 'oracle', targetDbType: ['mongodb', 'sqlserver', 'mysql', 'postgres', 'elasticsearch', 'kafka', 'clickhouse'], operator: 'regex', expression: '^INTERVAL DAY.* TO SECOND.*$', support: false},
    {sourceDbType: 'oracle', targetDbType: ['mongodb', 'sqlserver', 'mysql', 'postgres', 'elasticsearch', 'kafka', 'clickhouse'], operator: 'regex', expression: '^INTERVAL YEAR.* TO MONTH.*$', support: false},

    // source for mysql
    {sourceDbType: 'mysql', targetDbType: ['oracle', 'mongodb', 'sqlserver', 'postgres', 'elasticsearch', 'kafka', 'clickhouse'], operator: 'ignoreCaseIn', expression: ['GEOMETRY','POINT','LINESTRING','POLYGON','MULTIPOINT','MULTILINESTRING','MULTIPOLYGON','GEOMETRYCOLLECTION','DOUBLE UNSIGNED'], support: false},
    {sourceDbType: 'mysql', targetDbType: ['mysql'], operator: 'ignoreCaseIn', expression: ['GEOMETRY','POINT','LINESTRING','POLYGON','MULTIPOINT','MULTILINESTRING','MULTIPOLYGON','GEOMETRYCOLLECTION'], support: false},

    // source for sqlserver
    {sourceDbType: 'sqlserver', targetDbType: ['oracle', 'mongodb', 'mysql', 'postgres', 'elasticsearch', 'kafka'], operator: 'ignoreCaseIn', expression: ['xml', 'geometry', 'geography'], support: false},
    {sourceDbType: 'sqlserver', targetDbType: ['clickhouse'], operator: 'ignoreCaseIn', expression: ['xml', 'binary', 'varbinary', 'image'], support: false},

    // source for postgres
    {sourceDbType: 'postgres', targetDbType:['oracle', 'mongodb', 'sqlserver', 'mysql', 'elasticsearch', 'kafka', 'clickhouse'], operator: 'ignoreCaseIn', expression: ['point', 'line', 'lseg', 'box', 'path', 'polygon', 'circle', 'int4range', 'int8range', 'numrange', 'tsrange', 'tstzrange', 'daterange', 'macaddr8', 'uuid', 'xml'], support: false},
    {sourceDbType: 'postgres', targetDbType:['postgres'], operator: 'ignoreCaseIn', expression: ['int4range', 'int8range', 'numrange', 'tsrange', 'tstzrange', 'daterange'], support: false},
    {sourceDbType: 'postgres', targetDbType:['clickhouse'], operator: 'ignoreCaseIn', expression: ['bytea'], support: false},

    // source for mongodb
    {sourceDbType: 'mongodb', targetDbType:['oracle', 'mongodb', 'sqlserver', 'mysql', 'postgres', 'elasticsearch', 'kafka', 'clickhouse'], operator: 'ignoreCaseIn', expression: ['JAVASCRIPT', 'MIN_KEY', 'REGULAR_EXPRESSION', 'MAX_KEY'], support: false},
    {sourceDbType: 'mongodb', targetDbType:['clickhouse'], operator: 'ignoreCaseIn', expression: ['BINARY', 'NULL'], support: false}
];

db.dataTypeSupport.deleteMany({});
db.dataTypeSupport.insertMany(dataTypes);
