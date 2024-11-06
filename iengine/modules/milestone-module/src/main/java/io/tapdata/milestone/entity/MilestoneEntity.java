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
    private String errorCode;
    private String errorMessage;
    private String stackMessage;
    private Long begin;
    private Long end;
    private Long progress;
    private Long totals;

    private Boolean retrying;  // 是否正在重试
    private Long retryTimes;    // 重试次数
    private Long startRetryTs; // 开始重试时间
    private Long endRetryTs;   // 重试结束时间
    private Long nextRetryTs; // 下次重试时间
    private Long totalOfRetries;// 重试总次数
    private String retryOp;    // 操作代码，详细值参考 PDKMethod 或者自定义
    private Boolean retrySuccess;
    private Map<String, Object> retryMetadata;  // 可选，用于存储重试操作的上下文元信息

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

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getStackMessage() {
        return stackMessage;
    }

    public void setStackMessage(String stackMessage) {
        this.stackMessage = stackMessage;
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

    public Boolean getRetrying() {
        return retrying;
    }

    public void setRetrying(Boolean retrying) {
        this.retrying = retrying;
    }

    public Long getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(Long retryTimes) {
        this.retryTimes = retryTimes;
    }

    public Long getStartRetryTs() {
        return startRetryTs;
    }

    public void setStartRetryTs(Long startRetryTs) {
        this.startRetryTs = startRetryTs;
    }

    public Long getEndRetryTs() {
        return endRetryTs;
    }

    public void setEndRetryTs(Long endRetryTs) {
        this.endRetryTs = endRetryTs;
    }

    public Long getNextRetryTs() {
        return nextRetryTs;
    }

    public void setNextRetryTs(Long nextRetryTs) {
        this.nextRetryTs = nextRetryTs;
    }

    public Long getTotalOfRetries() {
        return totalOfRetries;
    }

    public void setTotalOfRetries(Long totalOfRetries) {
        this.totalOfRetries = totalOfRetries;
    }

    public String getRetryOp() {
        return retryOp;
    }

    public void setRetryOp(String retryOp) {
        this.retryOp = retryOp;
    }

    public Map<String, Object> getRetryMetadata() {
        return retryMetadata;
    }

    public void setRetryMetadata(Map<String, Object> retryMetadata) {
        this.retryMetadata = retryMetadata;
    }

    public Boolean getRetrySuccess() {
        return retrySuccess;
    }

    public void setRetrySuccess(Boolean retrySuccess) {
        this.retrySuccess = retrySuccess;
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
        Optional.ofNullable(map.get("errorCode")).ifPresent(v -> setErrorCode(v.toString()));
        Optional.ofNullable(map.get("stackMessage")).ifPresent(v -> setStackMessage(v.toString()));

        Optional.ofNullable(map.get("retrying")).ifPresent(v -> setRetrying((Boolean) v));
        Optional.ofNullable(map.get("retryTimes")).ifPresent(v -> setRetryTimes(Long.parseLong(v.toString())));
        Optional.ofNullable(map.get("startRetryTs")).ifPresent(v -> setStartRetryTs(Long.parseLong(v.toString())));
        Optional.ofNullable(map.get("endRetryTs")).ifPresent(v -> setEndRetryTs(Long.parseLong(v.toString())));
        Optional.ofNullable(map.get("nextRetryTs")).ifPresent(v -> setNextRetryTs(Long.parseLong(v.toString())));
        Optional.ofNullable(map.get("totalOfRetries")).ifPresent(v -> setTotalOfRetries(Long.parseLong(v.toString())));
        Optional.ofNullable(map.get("retryOp")).ifPresent(v -> setRetryOp(v.toString()));
        Optional.ofNullable(map.get("retryMetadata")).ifPresent(j -> setRetryMetadata((Map<String, Object>) j));
    }
}
