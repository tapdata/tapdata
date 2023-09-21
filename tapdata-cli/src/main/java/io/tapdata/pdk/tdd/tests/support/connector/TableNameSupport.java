package io.tapdata.pdk.tdd.tests.support.connector;

import java.util.Random;

/**
 * 实现类实现 命名规则 大写数据源名称 + “Support”  例如： MYSQLSupport.java
 */
public interface TableNameSupport {
    public final static int TABLE_NAME_LENGTH = 15;

    public default String tableName() {
        Random random = new Random();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < TABLE_NAME_LENGTH; i++) {
            builder.append((char) (97 + random.nextInt(26)));
        }
        return builder.toString();
    }

    public static TableNameSupport support(String connectorId) {
        Class clz = null;
        try {
            clz = Class.forName(TableNameSupport.class.getPackage().getName() + "." + connectorId.toUpperCase() + "Support");
            return (TableNameSupport) clz.newInstance();
        } catch (Exception e) {
            return new TableNameSupport() {
            };
        }
    }

    public static void main(String[] args) {
        Random random = new Random();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 12; i++) {
            builder.append((char) (97 + random.nextInt(26)));
        }
        System.out.println(builder.toString());
    }
}
