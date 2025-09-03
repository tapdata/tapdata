package com.tapdata.tm.apiCalls.vo;

import lombok.Data;

@Data
public class ApiCallStats {
    private String apiId;
    private String workOid;
    private long totalCount;
    private long okCount;
    private long notOkCount;

    // 构造器
    public ApiCallStats() {
    }

    // getter 和 setter
    public String getApiId() {
        return apiId;
    }

    public void setApiId(String apiId) {
        this.apiId = apiId;
    }

    public String getWorkOid() {
        return workOid;
    }

    public void setWorkOid(String workOid) {
        this.workOid = workOid;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public long getOkCount() {
        return okCount;
    }

    public void setOkCount(long okCount) {
        this.okCount = okCount;
    }

    public long getNotOkCount() {
        return notOkCount;
    }

    public void setNotOkCount(long notOkCount) {
        this.notOkCount = notOkCount;
    }

    @Override
    public String toString() {
        return String.format(
                "ApiCallStats{apiId='%s', workOid='%s', totalCount=%d, okCount=%d, notOkCount=%d}",
                apiId, workOid, totalCount, okCount, notOkCount
        );
    }
}