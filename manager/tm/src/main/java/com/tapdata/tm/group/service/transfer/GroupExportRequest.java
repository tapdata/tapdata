package com.tapdata.tm.group.service.transfer;

import com.tapdata.tm.group.dto.GroupInfoDto;
import com.tapdata.tm.group.dto.GroupInfoRecordDto;
import jakarta.servlet.http.HttpServletResponse;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

@Data
@AllArgsConstructor
public class GroupExportRequest {
    private HttpServletResponse response;
    private Map<String, byte[]> contents;
    private String name;
	private GroupInfoDto groupInfoDto;
	private String gitTag;
	private String gitBranchName;
	private String gitPrTitle;
	private String gitPrDescription;
	private GroupInfoRecordDto recordDto;
}