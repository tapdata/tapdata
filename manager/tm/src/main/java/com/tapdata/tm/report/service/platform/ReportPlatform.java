package com.tapdata.tm.report.service.platform;

public interface ReportPlatform {
    void sendRequest(String eventName, String params);
}
