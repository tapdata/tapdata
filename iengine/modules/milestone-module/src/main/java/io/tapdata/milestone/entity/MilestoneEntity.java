package io.tapdata.milestone.entity;

import io.tapdata.milestone.constants.MilestoneStatus;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/2/13 14:25 Create
 */
@Getter
@Setter
public class MilestoneEntity implements Serializable {
    private String code;
    private MilestoneStatus status;
    private String errorCode;
    private String errorMessage;
    private String stackMessage;
    private String[] dynamicDescriptionParameters;
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

    public synchronized void addProgress(int size) {
        this.progress = (this.progress == null ? 0 : this.progress) + size;
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
