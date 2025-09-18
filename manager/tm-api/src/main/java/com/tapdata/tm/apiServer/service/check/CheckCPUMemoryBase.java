package com.tapdata.tm.apiServer.service.check;

public interface CheckCPUMemoryBase extends ApiServerCheckBase {
    void check(UsageInfo usageInfo);
}
