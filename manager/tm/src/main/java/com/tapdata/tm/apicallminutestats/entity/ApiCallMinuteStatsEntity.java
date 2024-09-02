package com.tapdata.tm.apicallminutestats.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;


/**
 * ApiCallMinuteStats
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("ApiCallMinuteStats")
public class ApiCallMinuteStatsEntity extends BaseEntity {
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
	private Long responseDataRowTotalCount;
	// Total response time(milliseconds)
	private Long totalResponseTime;
	// Total size of data transferred by Api (Byte)
	private Long transferDataTotalBytes;
	// Response time per row: totalResponseTime / responseDataRowTotalCount
	private Double responseTimePerRow;
	// Number of rows per second: (transferDataTotalBytes / totalResponseTime) * 1000
	private Double transferBytePerSecond;
	/**
	 * Associated with {@link com.tapdata.tm.apiCalls.entity.ApiCallEntity}#id
	 */
	private String lastApiCallId;
}