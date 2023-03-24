/**
 * @title: DataFlowInsightStatisticsDto
 * @description:
 * @author lk
 * @date 2022/2/10
 */
package com.tapdata.tm.dataflowinsight.dto;

import java.math.BigInteger;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DataFlowInsightStatisticsDto {

	private String granularity;

	private List<DataStatisticInfo> inputDataStatistics;

	private BigInteger totalInputDataCount;


	@Getter
	@Setter
	@AllArgsConstructor
	public static class DataStatisticInfo{

		private String time;

		private BigInteger count;
	}
}
