package com.tapdata.tm.monitor.vo;

import lombok.Data;

import java.io.Serializable;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/22 10:59 Create
 */
@Data
public class SyncStatusStatisticsVo implements Serializable {
	private String _id; // 同步中为：表名，已完成为：**Done**，未同步为：**Wait**
	private Long counts; // 表数量
	private Long dataTotal; // 数据量
	private Long syncTotal; // 已同步数据量
}
