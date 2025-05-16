package com.tapdata.tm.taskinspect.vo;

import com.tapdata.tm.taskinspect.dto.TaskInspectHistoriesDto;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

/**
 * 任务内校验-运行指标
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/5/14 11:32 Create
 */
@Getter
@Setter
public class TaskInspectMetrics implements Serializable {

    private long cdcAccepts;    // 增量抽事件样数
    private long cdcIgnores;    // 增量忽略事件数
    private int diffChanges;    // 差异变更数（发现和恢复都递增）
    private long diffFirstTs;   // 第一条差异时间
    private long diffFromTs;    // 区间-开始时间
    private int diffFromTotals; // 区间-开始差异数
    private long diffToTs;      // 区间-结束时间
    private int diffToTotals;   // 区间-结束差异数

    public TaskInspectMetrics() {
    }

    public TaskInspectMetrics(long cdcAccepts, long cdcIgnores, int diffChanges, long diffFirstTs, long diffFromTs, int diffFromTotals, long diffToTs, int diffToTotals) {
        this.cdcAccepts = cdcAccepts;
        this.cdcIgnores = cdcIgnores;
        this.diffChanges = diffChanges;
        this.diffFirstTs = diffFirstTs;
        this.diffFromTs = diffFromTs;
        this.diffFromTotals = diffFromTotals;
        this.diffToTs = diffToTs;
        this.diffToTotals = diffToTotals;
    }

    /**
     * 将当前对象的属性值，填充到 Map 对象
     *
     * @param m 要填充的 Map 对象
     */
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

    /**
     * 从 Map 对象中获取对应的属性值，并将会上设置到一个新对象中
     *
     * @param m 存放值的 Map 对象
     * @return 新对象
     */
    public static TaskInspectMetrics fromMap(Map<?, ?> m) {
        TaskInspectMetrics metrics = new TaskInspectMetrics();
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
