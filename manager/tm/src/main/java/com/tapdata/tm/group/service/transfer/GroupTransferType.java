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
    FILE(false),
	GIT(true)
	;

	private final boolean async;

	GroupTransferType(boolean async) {
		this.async = async;
	}

}
