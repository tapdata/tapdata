package com.tapdata.tm.group.service.transfer;

import jakarta.servlet.http.HttpServletResponse;
import lombok.Data;

import java.util.Map;

@Data
public class GroupExportRequest {
    private HttpServletResponse response;
    private Map<String, byte[]> contents;
    private String name;

    public GroupExportRequest(HttpServletResponse response, Map<String, byte[]> contents, String name) {
        this.response = response;
        this.contents = contents;
        this.name = name;
    }
}
