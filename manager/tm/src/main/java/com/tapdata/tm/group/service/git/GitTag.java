package com.tapdata.tm.group.service.git;

import lombok.Data;

/**
 *
 * @author samuel
 * @Description
 * @create 2026-01-21 12:20
 **/
@Data
public class GitTag {
	private String tag;
	private long createTimestamp;
	private String commitSha;
}
