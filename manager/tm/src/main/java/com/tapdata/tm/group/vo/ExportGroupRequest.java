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
	/**
	 * 是否移除包内的敏感信息（连接凭据）。三态：
	 * {@code null} = 未指定（FILE 按保真、GIT 按脱敏）、{@code TRUE} = 要求脱敏、{@code FALSE} = 要求保真。
	 *
	 * 用 Boolean 而非 boolean，是为了把「显式要求保真」和「压根没提」分开 —— GIT 强制脱敏时
	 * 只有前者才需要回告「你的请求被覆盖了」（[ADR-0034] D2 不静默）。GIT 下本字段一律不生效。
	 */
	private Boolean removeSensitiveData;
	private Map<String, List<String>> groupResetTask;
	private String gitTag;
	private String gitBranchName;
	private String gitPrTitle;
	private String gitPrDescription;
}
