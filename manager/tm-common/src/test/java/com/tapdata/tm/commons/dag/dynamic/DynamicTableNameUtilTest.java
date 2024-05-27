package com.tapdata.tm.commons.dag.dynamic;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

class DynamicTableNameUtilTest {

    @Nested
    class GetDynamicTableTest {
        @Test
        void testDynamicTableNameByDefault() {
            SimpleDateFormat sdf = new SimpleDateFormat(DynamicTableNameByDate.DATE_FORMAT);
            String dateFormatStr1 = sdf.format(new Date());
            DynamicTableResult table = DynamicTableNameUtil.getDynamicTable("table", null);
            Assertions.assertNotNull(table);
            String dateFormatStr = sdf.format(new Date());
            if (dateFormatStr1.equals(dateFormatStr)) {
                Assertions.assertEquals("table_" + dateFormatStr, table.getDynamicName());
            }
            Assertions.assertEquals("table", table.getOldName());
        }
        @Test
        void testDynamicTableNameByPREFIX() {
            String tableName = "table";
            DynamicTableConfig rule = DynamicTableConfig.of().withCouplingLocation(CouplingLocation.PREFIX);
            SimpleDateFormat sdf = new SimpleDateFormat(DynamicTableNameByDate.DATE_FORMAT);
            String dateFormatStr1 = sdf.format(new Date());
            DynamicTableResult table = DynamicTableNameUtil.getDynamicTable(tableName, rule);
            Assertions.assertNotNull(table);
            String dateFormatStr = sdf.format(new Date());
            if (dateFormatStr.equals(dateFormatStr1)) {
                Assertions.assertEquals(dateFormatStr1 + DynamicTableStage.DEFAULT_COUPLING_SYMBOLS + tableName, table.getDynamicName());
            }
            Assertions.assertEquals(tableName, table.getOldName());
        }
        @Test
        void testDynamicTableNameByUnKnowRuleType() {
            String tableName = "table";
            DynamicTableConfig rule = DynamicTableConfig.of()
                    .withCouplingLocation(CouplingLocation.SUFFIX)
                    .withRuleType("xxx");
            rule.setCouplingLocation(CouplingLocation.SUFFIX);
            rule.setCouplingSymbols(null);
            rule.setParams(new HashMap<>());
            rule.setRuleType("xxx");
            SimpleDateFormat sdf = new SimpleDateFormat(DynamicTableNameByDate.DATE_FORMAT);
            String dateFormatStr = sdf.format(new Date());
            DynamicTableResult table = DynamicTableNameUtil.getDynamicTable(tableName, rule);
            Assertions.assertNotNull(table);
            String dateFormatStr1 = sdf.format(new Date());
            if (dateFormatStr.equals(dateFormatStr1)) {
                Assertions.assertEquals( tableName + DynamicTableStage.DEFAULT_COUPLING_SYMBOLS + dateFormatStr1, table.getDynamicName());
            }
            Assertions.assertEquals(tableName, table.getOldName());
            Assertions.assertNotNull(rule.getParams());
        }
        @Test
        void testDynamicTableNameByCouplingSymbols0() {
            String tableName = "table";
            DynamicTableConfig rule = DynamicTableConfig.of()
                    .withCouplingLocation(CouplingLocation.SUFFIX)
                    .withRuleType("default")
                    .withCouplingSymbols("0");
            SimpleDateFormat sdf = new SimpleDateFormat(DynamicTableNameByDate.DATE_FORMAT);
            String dateFormatStr = sdf.format(new Date());
            DynamicTableResult table = DynamicTableNameUtil.getDynamicTable(tableName, rule);
            Assertions.assertNotNull(table);
            String dateFormatStr1 = sdf.format(new Date());
            if (dateFormatStr.equals(dateFormatStr1)) {
                Assertions.assertEquals( tableName + "0" + dateFormatStr1, table.getDynamicName());
            }
            Assertions.assertEquals(tableName, table.getOldName());
        }
        @Test
        void testDynamicTableNameByCustomParams() {
            String tableName = "table";
            DynamicTableConfig rule = DynamicTableConfig.of()
                    .withCouplingLocation(CouplingLocation.SUFFIX)
                    .withRuleType("default")
                    .withCouplingSymbols("0")
                    .withParams(new HashMap<>());
            SimpleDateFormat sdf = new SimpleDateFormat(DynamicTableNameByDate.DATE_FORMAT);
            String dateFormatStr = sdf.format(new Date());
            DynamicTableResult table = DynamicTableNameUtil.getDynamicTable(tableName, rule);
            Assertions.assertNotNull(table);
            String dateFormatStr1 = sdf.format(new Date());
            if (dateFormatStr.equals(dateFormatStr1)) {
                Assertions.assertEquals( tableName + "0" + dateFormatStr1, table.getDynamicName());
            }
            Assertions.assertEquals(tableName, table.getOldName());
        }
    }
}