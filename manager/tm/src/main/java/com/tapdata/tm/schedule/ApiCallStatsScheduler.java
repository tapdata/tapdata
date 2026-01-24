package com.tapdata.tm.schedule;

import com.tapdata.tm.apiCalls.service.SupplementApiCallServer;
import com.tapdata.tm.apiCalls.service.WorkerCallServiceImpl;
import com.tapdata.tm.v2.api.monitor.service.ApiMetricsRawScheduleExecutor;
import com.tapdata.tm.v2.api.usage.service.ServerUsageMetricScheduleExecutor;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerService;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.core.annotation.Order;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2024-08-27 10:30
 **/
@Component
@Slf4j
@Order
public class ApiCallStatsScheduler {

	@Resource(name = "apiMetricsRawScheduleExecutor")
	ApiMetricsRawScheduleExecutor service;

	@Resource(name = "serverUsageMetricScheduleExecutor")
	ServerUsageMetricScheduleExecutor usageMetricScheduleExecutor;

	@Resource(name = "workerServiceImpl")
	private WorkerService workerService;

	@Resource(name = "supplementApiCallServer")
	private SupplementApiCallServer supplementApiCallServer;

	@Resource(name = "workerCallServiceImpl")
	WorkerCallServiceImpl workerCallServiceImpl;


	public ApiCallStatsScheduler() {

	}


	@Scheduled(cron = "0/10 * * * * ?")
	@SchedulerLock(name = "server_usage_stats_scheduler", lockAtMostFor = "30m", lockAtLeastFor = "5s")
	public void scheduleForApiServerUsage() {
		try {
			usageMetricScheduleExecutor.aggregateUsage();
		} catch (Exception e) {
			log.warn("Aggregate api server usage failed, will skip it, error: {}", e.getMessage(), e);
		}
	}

	@Scheduled(fixedDelay = 5000L, initialDelay = 0L)
	@SchedulerLock(name = "api_call_metric_stats_scheduler_task", lockAtMostFor = "30m", lockAtLeastFor = "10s")
	public void scheduleForApiCall() {
		try {
			service.aggregateApiCall();
		} catch (Exception e) {
			log.warn("Aggregate ApiCall failed, will skip it, error: {}", e.getMessage(), e);
		}

//		try {
//			workerCallServiceImpl.metric();
//		} catch (Exception e) {
//			log.error("Aggregate ApiCallMinuteStats failed, error: {}", e.getMessage(), e);
//		}
//
//		try {
//			collectOnceApiCountOfWorker();
//		} catch (Exception e) {
//			log.error("Aggregate API call count of worker failed, error: {}", e.getMessage(), e);
//		}
//		supplement();
	}

	void collectOnceApiCountOfWorker() {
		//query all server
		List<WorkerDto> all = workerService.findAll(Query.query(
				Criteria.where("worker_type").is("api-server")
						.and("delete").ne(true)));
		if (null == all || all.isEmpty()) {
			return;
		}
		all.forEach(w -> {
			try {
				workerCallServiceImpl.collectApiCallCountGroupByWorker(w.getProcessId());
			} catch (Exception e) {
				log.error("Unable to perform Worker level request access data statistics on API servers", e);
			}
		});
	}

	void supplement() {
		try {
			supplementApiCallServer.supplementOnce();
		} catch (Exception e) {
			log.error("Abnormal historical supplementary data statistics, error: {}", e.getMessage(), e);
		}
	}
}
