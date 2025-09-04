package com.tapdata.tm.apicallstats.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashSet;
import java.util.Set;


/**
 * ApiCallStats
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ApiCallStatsDto extends BaseDto {
	private String moduleId;
	// Total number of API calls
	private Long callTotalCount = 0L;
	// Total size of data transferred by Api (Byte)
	private Long transferDataTotalBytes = 0L;
	// Total number of API access alarms
	private Long callAlarmTotalCount = 0L;
	// Total number of response data rows accessed by API
	private Long responseDataRowTotalCount = 0L;
	// Access failure rate
	private Double accessFailureRate = 0D;
	// Total response time(milliseconds)
	private Long totalResponseTime = 0L;
	// Max response time(milliseconds)
	private Long maxResponseTime;
	// Number of APIs with warning calls(As long as the alarm is called once)
	private Long alarmApiTotalCount = 0L;
	/**
	 * Client id list(associated with {@link com.tapdata.tm.apiCalls.entity.ApiCallEntity}#user_info#clientId)
	 */
	private Set<String> clientIds = new HashSet<>();
	/**
	 * Associated with {@link com.tapdata.tm.apiCalls.entity.ApiCallEntity}#id
	 * The last API call ID
	 * Used to query the last API call record as an offset
	 */
	private String lastApiCallId;
}