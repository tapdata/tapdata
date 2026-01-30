package com.tapdata.tm.group.vo;

import com.tapdata.tm.group.service.transfer.GroupTransferType;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 *
 * @author samuel
 * @Description
 * @create 2026-01-21 11:24
 **/
@Data
public class ExportGroupRequest {
	private List<String> groupIds;
	private GroupTransferType groupTransferType = GroupTransferType.FILE;
	private Map<String, List<String>> groupResetTask;
	private String gitTag;
}
