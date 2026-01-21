package com.tapdata.tm.schedule;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;

/**
 * @author samuel
 * @Description
 * @create 2024-08-30 18:28
 **/
@DisplayName("Class ApiCallStatsScheduler Test")
class ApiCallStatsSchedulerTest {

	private ApiCallStatsScheduler apiCallStatsScheduler;

	@BeforeEach
	void setUp() {
		apiCallStatsScheduler = new ApiCallStatsScheduler();
	}
}