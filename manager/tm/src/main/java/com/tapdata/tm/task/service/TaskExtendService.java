package com.tapdata.tm.task.service;

public interface TaskExtendService {
    void stopTaskByAgentIdAndUserId(String agentId, String userId);

    void clearFunctionRetry();
}
