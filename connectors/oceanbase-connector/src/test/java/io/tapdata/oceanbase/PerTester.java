package io.tapdata.oceanbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;

public class PerTester {
    public static void main(String[] args) throws Exception {
        String url = System.getProperty("url", "jdbc:mysql://localhost:2881/target");
        String user = System.getProperty("user", "root");
        String password = System.getProperty("password", "root");

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            String tableName = "hstest0920_100fields";
            Map<String, Object> lastPk = new HashMap<>();
            List<FieldStruct> fields = new ArrayList<>();
            fields.add(FieldStruct.Type.Int.field("id", true));
            fields.add(FieldStruct.Type.String.field("title", false));
            fields.add(FieldStruct.Type.Date.field("created", false));
            for (int i = 97; i > 0; i--) {
                fields.add(FieldStruct.Type.String.field("f" + i, false));
            }

            String createTableSQL = createTableSQL(tableName, fields);
            try (Statement s = conn.createStatement()) {
                System.out.println("create table: " + s.execute(createTableSQL));
            }

            for (int i = 0; i < 10; i++) {
                List<Map<String, Object>> dataList = buildDataList(lastPk, fields, 10);
                String sql = insertUpdate(tableName, keys(fields), dataList);
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    long counts = 0;
                    for (int size : ps.executeBatch()) {
                        counts += size;
                    }
                    for (Map<String, Object> data : dataList) {
                        int idx = 1;
                        for (Map.Entry<String, Object> en : data.entrySet()) {
                            ps.setObject(idx++, en.getValue());
                        }
                        ps.addBatch();
                    }
                    for (int size : ps.executeBatch()) {
                        counts += size;
                    }
                    System.out.println("build sql(" + counts + "): " + sql);
                    ps.clearParameters();
                }
            }

        }
    }

    private static String insertUpdate(String tableName, Set<String> keys, List<Map<String, Object>> dataList) {
        StringBuilder sql = new StringBuilder();

        sql.append("insert into `").append(tableName).append("`(");
        Set<String> fields = dataList.get(0).keySet();
        for (String key : fields) sql.append("`").append(key).append("`,");
        sql.setLength(sql.length() - 1);
        sql.append(")");

        sql.append(" values(");
        for (String key : fields) sql.append("?,");
        sql.setLength(sql.length() - 1);
        sql.append(")");

        sql.append(" ON DUPLICATE KEY UPDATE ");
        for (String key : fields) {
            if (keys.contains(key)) continue; // 忽略主键
            sql.append("`").append(key).append("`=values(`").append(key).append("`),");
        }
        sql.setLength(sql.length() - 1);

        return sql.toString();
    }

    private static String createTableSQL(String tableName, List<FieldStruct> fields) {
        StringBuilder sql = new StringBuilder();
        sql.append("create table if not exists `").append(tableName).append("`(");

        for (FieldStruct field : fields) {
            sql.append(field.name).append(" ");
            switch (field.type) {
                case Int:
                    sql.append("int");
                    break;
                case Date:
                    sql.append("datetime");
                    break;
                case String:
                    sql.append("varchar(64)");
                    break;
                default:
                    throw new RuntimeException("field type not support: " + field.type);
            }
            if (field.isPk) {
                sql.append(" primary key");
            }
            sql.append(",");
        }
        sql.setLength(sql.length() - 1);

        sql.append(")");
        return sql.toString();
    }

    private static class FieldStruct {
        enum Type {
            Int, String, Date,
            ;

            FieldStruct field(String name, boolean primaryKey) {
                return new FieldStruct(name, this, primaryKey);
            }
        }

        String name;
        Type type;
        boolean isPk;

        public FieldStruct(String name, Type type, boolean isPk) {
            this.name = name;
            this.type = type;
            this.isPk = isPk;
        }
    }

    private static Set<String> keys(List<FieldStruct> fields) {
        Set<String> keys = new LinkedHashSet<>();
        for (FieldStruct field : fields) {
            if (field.isPk) {
                keys.add(field.name);
            }
        }
        return keys;
    }

    private static Map<String, Object> buildData(Map<String, Object> lastPk, List<FieldStruct> fields) {
        Map<String, Object> data = new LinkedHashMap<>();
        for (FieldStruct field : fields) {
            switch (field.type) {
                case Int:
                    if (field.isPk) {
                        Object v = lastPk.computeIfAbsent(field.name, k -> 1);
                        if (v instanceof Integer) {
                            v = ((Integer) v) + 1;
                        } else {
                            v = 1;
                        }
                        lastPk.put(field.name, v);
                        data.put(field.name, v);
                    } else {
                        data.put(field.name, 1);
                    }
                    break;
                case Date:
                    data.put(field.name, new Date());
                    break;
                case String:
                    if (field.isPk) {
                        Object v = lastPk.computeIfAbsent(field.name, k -> 1);
                        if (v instanceof Integer) {
                            v = ((Integer) v) + 1;
                        } else {
                            v = 1;
                        }
                        lastPk.put(field.name, v);
                        data.put(field.name, "hello-" + v);
                    } else {
                        data.put(field.name, "hello");
                    }
                    break;
                default:
                    throw new RuntimeException("field type not support: " + field.type);
            }
        }

        return data;
    }

    private static List<Map<String, Object>> buildDataList(Map<String, Object> lastPk, List<FieldStruct> fields, int size) {
        List<Map<String, Object>> list = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            list.add(buildData(lastPk, fields));
        }

        return list;
    }

}
