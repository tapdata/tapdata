package com.tapdata.tm.commons.dag.process.duck;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

class JoinInfoTest {

    @Test
    void toMapShouldConvertJoinInfosByTable() {
        JoinInfo users = joinInfo("users", joinField("orders", "user_id"), joinField("users", "id"));
        JoinInfo payments = joinInfo("payments", null, joinField("payments", "order_id"));

        Map<String, List<Map<String, Object>>> map = JoinInfo.toMap(List.of(users, payments));

        Assertions.assertEquals(2, map.size());
        Assertions.assertEquals("orders", ((Map<?, ?>) map.get("users").get(0).get(JoinInfo.LEFT)).get(JoinField.TABLE));
        Assertions.assertEquals("id", ((Map<?, ?>) map.get("users").get(0).get(JoinInfo.RIGHT)).get(JoinField.FIELD));
        Assertions.assertFalse(map.get("payments").get(0).containsKey(JoinInfo.LEFT));
        Assertions.assertTrue(map.get("payments").get(0).containsKey(JoinInfo.RIGHT));
    }

    @Test
    void getJoinKeysShouldReturnEmptyForInvalidShape() {
        Assertions.assertTrue(JoinInfo.getJoinKeys(Map.of(), "orders").isEmpty());
        Assertions.assertTrue(JoinInfo.getJoinKeys(Map.of(JoinInfo.TABLE_PROPS, "bad"), "orders").isEmpty());
        Assertions.assertTrue(JoinInfo.getJoinKeys(Map.of(JoinInfo.TABLE_PROPS, Map.of()), "orders").isEmpty());
        Assertions.assertTrue(JoinInfo.getJoinKeys(Map.of(
                JoinInfo.TABLE_PROPS, Map.of(JoinInfo.JOIN_KEY_INFO, "bad")
        ), "orders").isEmpty());
    }

    @Test
    void getJoinKeysShouldCollectUniqueFieldsForTable() {
        Map<String, Object> leftOrders = new java.util.HashMap<>(joinField("orders", "user_id").toMap());
        Map<String, Object> rightOrders = new java.util.HashMap<>(joinField("orders", "tenant_id").toMap());
        Map<String, Object> otherTable = new java.util.HashMap<>(joinField("users", "id").toMap());
        Map<String, Object> key1 = Map.of(JoinInfo.LEFT, leftOrders, JoinInfo.RIGHT, otherTable);
        Map<String, Object> key2 = Map.of(JoinInfo.LEFT, rightOrders, JoinInfo.RIGHT, leftOrders);
        Map<String, Object> tableAttr = Map.of(
                JoinInfo.TABLE_PROPS,
                Map.of(JoinInfo.JOIN_KEY_INFO, Map.of(
                        "users", List.of(key1, "bad"),
                        "payments", List.of(key2),
                        "ignored", "bad"
                ))
        );

        List<String> keys = JoinInfo.getJoinKeys(tableAttr, "orders");

        Assertions.assertEquals(2, keys.size());
        Assertions.assertTrue(keys.contains("user_id"));
        Assertions.assertTrue(keys.contains("tenant_id"));
    }

    @Test
    void fieldsOfShouldHandleInvalidInfo() {
        Assertions.assertNull(JoinField.fieldsOf(null, "orders"));
        Assertions.assertNull(JoinField.fieldsOf("bad", "orders"));
        Assertions.assertNull(JoinField.fieldsOf(Map.of(JoinField.TABLE, "users", JoinField.FIELD, "id"), "orders"));
        Assertions.assertEquals("id", JoinField.fieldsOf(Map.of(JoinField.TABLE, "orders", JoinField.FIELD, "id"), "orders"));
    }

    @Test
    void constantsShouldRemainStable() {
        Assertions.assertEquals("TABLE_PROPS", JoinInfo.TABLE_PROPS);
        Assertions.assertEquals("PK_INFO", JoinInfo.PK_INFO);
        Assertions.assertEquals("JOIN_KEY_INFO", JoinInfo.JOIN_KEY_INFO);
        Assertions.assertEquals("TABLE_ALAIN_NAME", JoinInfo.TABLE_ALAIN_NAME);
        Assertions.assertEquals("PK", JoinInfo.PK);
        Assertions.assertEquals("left", JoinInfo.LEFT);
        Assertions.assertEquals("right", JoinInfo.RIGHT);
    }

    private static JoinInfo joinInfo(String table, JoinField left, JoinField right) {
        JoinKeyPair pair = new JoinKeyPair();
        pair.setLeft(left);
        pair.setRight(right);
        JoinInfo joinInfo = new JoinInfo();
        joinInfo.setTable(table);
        joinInfo.setJoinType("LEFT");
        joinInfo.setJoinKeys(List.of(pair));
        return joinInfo;
    }

    private static JoinField joinField(String table, String field) {
        JoinField joinField = new JoinField();
        joinField.setTable(table);
        joinField.setField(field);
        return joinField;
    }
}
