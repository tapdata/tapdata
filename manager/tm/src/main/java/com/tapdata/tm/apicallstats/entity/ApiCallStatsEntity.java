package com.tapdata.tm.apicallstats.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Set;


/**
 * ApiCallStats
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("ApiCallStats")
public class ApiCallStatsEntity extends BaseEntity {
	/**
	 * Associated with {@link com.tapdata.tm.modules.entity.ModulesEntity}#id
	 */
	private String moduleId;
	// Total number of API calls
	private Long callTotalCount;
	// Total size of data transferred by Api (Byte)
	private Long transferDataTotalBytes;
	// Total number of API access alarms
	private Long callAlarmTotalCount;
	// Total number of response data rows accessed by API
	private Long responseDataRowTotalCount;
	// Access failure rate
	private Double accessFailureRate;
	// Total response time(milliseconds)
	private Long totalResponseTime;
	// Max response time(milliseconds)
	private Long maxResponseTime;
	/**
	 * Client id list(associated with {@link com.tapdata.tm.apiCalls.entity.ApiCallEntity}#user_info#clientId)
	 */
	private Set<String> clientIds;
	/**
	 * Associated with {@link com.tapdata.tm.apiCalls.entity.ApiCallEntity}#id
	 * The last API call ID
	 * Used to query the last API call record as an offset
	 */
	private String lastApiCallId;
}