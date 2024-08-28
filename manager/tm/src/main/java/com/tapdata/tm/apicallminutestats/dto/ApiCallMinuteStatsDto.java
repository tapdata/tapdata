package com.tapdata.tm.apicallminutestats.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;


/**
 * ApiCallMinuteStats
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ApiCallMinuteStatsDto extends BaseDto {
	/**
	 * Associated with {@link com.tapdata.tm.modules.entity.ModulesEntity}#id
	 */
	private String moduleId;
	/**
	 * API call time
	 * Only keep year, month, day, hour, minute
	 * Example: 2021-08-29T10:01:00.000Z
	 */
	private Date apiCallTime;
	// Total number of response data rows accessed by API
	private Long responseDataRowTotalCount = 0L;
	// Total response time(milliseconds)
	private Long totalResponseTime = 0L;
	// Response time per row: totalResponseTime / responseDataRowTotalCount
	private Double responseTimePerRow = 0D;
	// Number of rows per second: (responseDataRowTotalCount / totalResponseTime) * 1000
	private Double rowPerSecond = 0D;
	/**
	 * Associated with {@link com.tapdata.tm.apiCalls.entity.ApiCallEntity}#id
	 */
	private String lastApiCallId;
}