package com.tapdata.tm.taskinspect.vo;

import com.tapdata.tm.taskinspect.dto.TaskInspectHistoriesDto;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

/**
 * 统计范围时间内的差异指标
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/5/14 11:32 Create
 */
@Getter
@Setter
public class CheckQueueMetrics implements Serializable {

    private long cdcAccepts;
    private long cdcIgnores;
    private int diffChanges;
    private long diffFirstTs;
    private long diffFromTs;
    private int diffFromTotals;
    private long diffToTs;
    private int diffToTotals;

    public CheckQueueMetrics() {
    }

    public CheckQueueMetrics(long cdcAccepts, long cdcIgnores, int diffChanges, long diffFirstTs, long diffFromTs, int diffFromTotals, long diffToTs, int diffToTotals) {
        this.cdcAccepts = cdcAccepts;
        this.cdcIgnores = cdcIgnores;
        this.diffChanges = diffChanges;
        this.diffFirstTs = diffFirstTs;
        this.diffFromTs = diffFromTs;
        this.diffFromTotals = diffFromTotals;
        this.diffToTs = diffToTs;
        this.diffToTotals = diffToTotals;
    }

    public void toMap(Map<String, Object> m) {
        m.put(TaskInspectHistoriesDto.FIELD_ATTRS_CDC_ACCEPTS, getCdcAccepts());
        m.put(TaskInspectHistoriesDto.FIELD_ATTRS_CDC_IGNORES, getCdcIgnores());
        m.put(TaskInspectHistoriesDto.FIELD_ATTRS_DIFF_CHANGES, getDiffChanges());
        m.put(TaskInspectHistoriesDto.FIELD_ATTRS_DIFF_FIRST_TS, getDiffFirstTs());
        m.put(TaskInspectHistoriesDto.FIELD_ATTRS_DIFF_FROM_TS, getDiffFromTs());
        m.put(TaskInspectHistoriesDto.FIELD_ATTRS_DIFF_FROM_TOTALS, getDiffFromTotals());
        m.put(TaskInspectHistoriesDto.FIELD_ATTRS_DIFF_TO_TS, getDiffToTs());
        m.put(TaskInspectHistoriesDto.FIELD_ATTRS_DIFF_TO_TOTALS, getDiffToTotals());
    }

    public static CheckQueueMetrics fromMap(Map<?, ?> m) {
        CheckQueueMetrics metrics = new CheckQueueMetrics();
        metrics.setCdcAccepts(getCdcAccepts(m));
        metrics.setCdcIgnores(getCdcIgnores(m));
        metrics.setDiffChanges(getDiffChanges(m));
        metrics.setDiffFirstTs(getDiffFirstTs(m));
        metrics.setDiffFromTs(getDiffFromTs(m));
        metrics.setDiffFromTotals(getDiffFromTotals(m));
        metrics.setDiffToTs(getDiffToTs(m));
        metrics.setDiffToTotals(getDiffToTotals(m));
        return metrics;
    }

    public static long getCdcAccepts(Map<?, ?> m) {
        if (m.get(TaskInspectHistoriesDto.FIELD_ATTRS_CDC_ACCEPTS) instanceof Long v) {
            return v;
        }
        return 0L;
    }

    public static long getCdcIgnores(Map<?, ?> m) {
        if (m.get(TaskInspectHistoriesDto.FIELD_ATTRS_CDC_IGNORES) instanceof Long v) {
            return v;
        }
        return 0L;
    }

    public static int getDiffChanges(Map<?, ?> m) {
        if (m.get(TaskInspectHistoriesDto.FIELD_ATTRS_DIFF_CHANGES) instanceof Integer v) {
            return v;
        }
        return 0;
    }

    public static long getDiffFirstTs(Map<?, ?> m) {
        if (m.get(TaskInspectHistoriesDto.FIELD_ATTRS_DIFF_FIRST_TS) instanceof Long v) {
            return v;
        }
        return 0L;
    }

    public static long getDiffFromTs(Map<?, ?> m) {
        if (m.get(TaskInspectHistoriesDto.FIELD_ATTRS_DIFF_FROM_TS) instanceof Long v) {
            return v;
        }
        return 0L;
    }

    public static int getDiffFromTotals(Map<?, ?> m) {
        if (m.get(TaskInspectHistoriesDto.FIELD_ATTRS_DIFF_FROM_TOTALS) instanceof Integer v) {
            return v;
        }
        return 0;
    }

    public static long getDiffToTs(Map<?, ?> m) {
        if (m.get(TaskInspectHistoriesDto.FIELD_ATTRS_DIFF_TO_TS) instanceof Long v) {
            return v;
        }
        return 0L;
    }

    public static int getDiffToTotals(Map<?, ?> m) {
        if (m.get(TaskInspectHistoriesDto.FIELD_ATTRS_DIFF_TO_TOTALS) instanceof Integer v) {
            return v;
        }
        return 0;
    }
}
