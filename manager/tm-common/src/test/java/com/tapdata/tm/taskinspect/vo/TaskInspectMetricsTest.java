package com.tapdata.tm.taskinspect.vo;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 检查队列指标-测试
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/5/14 14:03 Create
 */
class TaskInspectMetricsTest {

    @Test
    void testConvert() {
        int cdcAccepts = 100;
        int cdcIgnores = 50;
        int diffChange = 1;
        long diffFirstTs = 1234567890L;
        long diffFromTs = 1234567890L;
        int diffFromTotals = 100;
        long diffToTs = 1234567890L;
        int diffToTotals = 100;

        // 赋值测试
        TaskInspectMetrics metrics = new TaskInspectMetrics(cdcAccepts, cdcIgnores, diffChange, diffFirstTs, diffFromTs, diffFromTotals, diffToTs, diffToTotals);
        assertEquals(cdcAccepts, metrics.getCdcAccepts());
        assertEquals(cdcIgnores, metrics.getCdcIgnores());
        assertEquals(diffChange, metrics.getDiffChanges());
        assertEquals(diffFirstTs, metrics.getDiffFirstTs());
        assertEquals(diffFromTs, metrics.getDiffFromTs());
        assertEquals(diffFromTotals, metrics.getDiffFromTotals());
        assertEquals(diffToTs, metrics.getDiffToTs());
        assertEquals(diffToTotals, metrics.getDiffToTotals());

        // 双向转换验证
        Map<String, Object> map = new HashMap<>();
        metrics.toMap(map);
        TaskInspectMetrics newMetrics = TaskInspectMetrics.fromMap(map);
        assertEquals(newMetrics.getCdcAccepts(), metrics.getCdcAccepts());
        assertEquals(newMetrics.getCdcIgnores(), metrics.getCdcIgnores());
        assertEquals(newMetrics.getDiffChanges(), metrics.getDiffChanges());
        assertEquals(newMetrics.getDiffFirstTs(), metrics.getDiffFirstTs());
        assertEquals(newMetrics.getDiffFromTs(), metrics.getDiffFromTs());
        assertEquals(newMetrics.getDiffFromTotals(), metrics.getDiffFromTotals());
        assertEquals(newMetrics.getDiffToTs(), metrics.getDiffToTs());
        assertEquals(newMetrics.getDiffToTotals(), metrics.getDiffToTotals());
    }

    @Test
    void testDefaultValueFromMap() {
        Map<String, Object> map = new HashMap<>();
        TaskInspectMetrics newMetrics = TaskInspectMetrics.fromMap(map);
        assertEquals(0L, newMetrics.getCdcAccepts());
        assertEquals(0L, newMetrics.getCdcIgnores());
        assertEquals(0, newMetrics.getDiffChanges());
        assertEquals(0L, newMetrics.getDiffFirstTs());
        assertEquals(0L, newMetrics.getDiffFromTs());
        assertEquals(0, newMetrics.getDiffFromTotals());
        assertEquals(0L, newMetrics.getDiffToTs());
        assertEquals(0, newMetrics.getDiffToTotals());
    }
}
