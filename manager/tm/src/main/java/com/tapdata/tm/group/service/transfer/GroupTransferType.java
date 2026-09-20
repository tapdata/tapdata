package com.tapdata.tm.group.service.transfer;

import lombok.Getter;

/**
 * 分组传输类型枚举
 * 定义支持的导入导出方式
 */
@Getter
public enum GroupTransferType {
    /**
     * 文件传输（tar 包）
     */
    FILE(false, false),
	GIT(true, true)
	;

	private final boolean async;

	/**
	 * 该通路是否**强制**脱敏，且不可被入参绕过（[ADR-0034] D2）。
	 *
	 * GIT 为 true：包会进 git 历史，而 git 历史永久留存、可被 fork/缓存 —— 明文凭据一旦推上去
	 * 就收不回来。新增任何「把包送出本平台」的通路时，这一位必须显式想清楚再填。
	 */
	private final boolean forceMaskSecrets;

	GroupTransferType(boolean async, boolean forceMaskSecrets) {
		this.async = async;
		this.forceMaskSecrets = forceMaskSecrets;
	}

}
