/**
 * @title: RuntimeMonitorResp
 * @description:
 * @author lk
 * @date 2021/10/29
 */
package com.tapdata.tm.dataflowinsight.dto;

import java.util.Date;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RuntimeMonitorResp {

	private String statsType;

	private Date createTime;

	private String dataFlowId;

	private String stageId;

	private String granularity;

	private Object statsData;

}
