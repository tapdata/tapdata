package io.tapdata.milestone.entity;

import io.tapdata.milestone.constants.MilestoneStatus;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/2/13 14:25 Create
 */
public class MilestoneEntity implements Serializable {
    private String code;
    private MilestoneStatus status;
    private String errorMessage;
    private Long begin;
    private Long end;
    private Long progress;
    private Long totals;

    public MilestoneEntity() {
    }

    public MilestoneEntity(String code, MilestoneStatus status) {
        this.code = code;
        this.status = status;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public MilestoneStatus getStatus() {
        return status;
    }

    public void setStatus(MilestoneStatus status) {
        this.status = status;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public Long getBegin() {
        return begin;
    }

    public void setBegin(Long begin) {
        this.begin = begin;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }

    public Long getProgress() {
        return progress;
    }

    public void setProgress(Long progress) {
        this.progress = progress;
    }

    public synchronized void addProgress(int size) {
        this.progress += size;
    }

    public Long getTotals() {
        return totals;
    }

    public void setTotals(Long totals) {
        this.totals = totals;
    }

    public static MilestoneEntity valueOf(Object o) {
        if (o instanceof Map) {
            MilestoneEntity entity = new MilestoneEntity();
            entity.setOfMap((Map<String, Object>) o);
            return entity;
        }
        return null;
    }

    public void setOfMap(Map<String, Object> map) {
        Optional.ofNullable(map.get("status")).ifPresent(v -> setStatus(MilestoneStatus.valueOf(v.toString())));
        Optional.ofNullable(map.get("begin")).ifPresent(v -> setBegin(Long.parseLong(v.toString())));
        Optional.ofNullable(map.get("end")).ifPresent(v -> setEnd(Long.parseLong(v.toString())));
        Optional.ofNullable(map.get("progress")).ifPresent(v -> setProgress(Long.parseLong(v.toString())));
        Optional.ofNullable(map.get("totals")).ifPresent(v -> setTotals(Long.parseLong(v.toString())));
        Optional.ofNullable(map.get("errorMessage")).ifPresent(v -> setErrorMessage(v.toString()));
    }
}
