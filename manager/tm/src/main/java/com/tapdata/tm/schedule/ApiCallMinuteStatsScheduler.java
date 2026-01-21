package com.tapdata.tm.schedule;

import com.tapdata.tm.apiCalls.service.SupplementApiCallServer;
import com.tapdata.tm.apiCalls.service.WorkerCallServiceImpl;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Autowired;
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
public class ApiCallMinuteStatsScheduler {
	private final WorkerCallServiceImpl workerCallServiceImpl;
	private final WorkerService workerService;
	private final SupplementApiCallServer supplementApiCallServer;

	@Autowired
	public ApiCallMinuteStatsScheduler(WorkerCallServiceImpl workerCallServiceImpl,
									   WorkerService ws,
									   SupplementApiCallServer sas) {
		this.workerCallServiceImpl = workerCallServiceImpl;
		this.workerService = ws;
		this.supplementApiCallServer = sas;
	}

	@Scheduled(fixedDelay = 5000L, initialDelay = 0L)
	@SchedulerLock(name = "api_call_worker_minute_stats_scheduler", lockAtMostFor = "20m", lockAtLeastFor = "10s")
	public void scheduleWorkerCall() {
		try {
			workerCallServiceImpl.metric();
		} catch (Exception e) {
			log.error("Aggregate ApiCallMinuteStats failed, error: {}", e.getMessage(), e);
		}

		try {
			collectOnceApiCountOfWorker();
		} catch (Exception e) {
			log.error("Aggregate API call count of worker failed, error: {}", e.getMessage(), e);
		}
		supplement();
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
