package com.tapdata.tm.schedule;

import com.tapdata.tm.apiCalls.service.ApiCallService;
import com.tapdata.tm.apiCalls.service.SupplementApiCallServer;
import com.tapdata.tm.apiCalls.service.WorkerCallServiceImpl;
import com.tapdata.tm.apicallminutestats.dto.ApiCallMinuteStatsDto;
import com.tapdata.tm.apicallminutestats.entity.ApiCallMinuteStatsEntity;
import com.tapdata.tm.apicallminutestats.service.ApiCallMinuteStatsService;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2024-08-27 10:30
 **/
@Component
@Slf4j
public class ApiCallMinuteStatsScheduler {

	private final ModulesService modulesService;
	private final ApiCallMinuteStatsService apiCallMinuteStatsService;
	private final ApiCallService apiCallService;
	private final WorkerCallServiceImpl workerCallServiceImpl;
	private final WorkerService workerService;
	private final SupplementApiCallServer supplementApiCallServer;

	@Autowired
	public ApiCallMinuteStatsScheduler(ModulesService modulesService,
									   ApiCallMinuteStatsService apiCallMinuteStatsService,
									   ApiCallService apiCallService,
									   WorkerCallServiceImpl workerCallServiceImpl,
									   WorkerService ws,
									   SupplementApiCallServer sas) {
		this.modulesService = modulesService;
		this.apiCallMinuteStatsService = apiCallMinuteStatsService;
		this.apiCallService = apiCallService;
		this.workerCallServiceImpl = workerCallServiceImpl;
		this.workerService = ws;
		this.supplementApiCallServer = sas;
	}

	/**
	 * Scheduled task to aggregate the API call data of each module and save it to ApiCallMinuteStats
	 */
	@Scheduled(cron = "0 0/1 * * * ?")
	@SchedulerLock(name = "api_call_minute_stats_scheduler", lockAtMostFor = "30m", lockAtLeastFor = "5s")
	public void schedule() {
		Thread.currentThread().setName(getClass().getSimpleName() + "-scheduler");
		if (log.isDebugEnabled()) {
			log.debug("Start to aggregate ApiCallMinuteStats...");
		}

		// Get all Modules, excluding deleted ones
		Query modulesQuery = new Query(Criteria.where("is_deleted").ne(true));
		modulesQuery.fields().include("id", "user_id");
		List<ModulesDto> modulesList = modulesService.findAll(modulesQuery);
		if (log.isDebugEnabled()) {
			log.debug("Found all modules size: {}, include fields: {}", modulesList.size(), modulesQuery.getFieldsObject().toJson());
		}

		// Traverse all Modules, perform pre-aggregation, and save to ApiCallMinuteStats
		int traverseStep = 0;
		// Because the expiration time defined by the ttl index is 2 hours, only the data for the last 2 hours can be counted.
		Instant startInstant = Instant.now().minus(2, ChronoUnit.HOURS);
		for (ModulesDto modulesDto : modulesList) {
			long loopStartMs = System.currentTimeMillis();
			ObjectId moduleOid = modulesDto.getId();
			if (null == moduleOid) {
				continue;
			}
			String moduleId = moduleOid.toString();
			traverseStep++;
			// Get the historical ApiCallMinuteStats record based on moduleId, and get the lastApiCallId from it as this offset
			Query apiCallStatsQuery = Query.query(Criteria.where("moduleId").is(moduleId)).with(Sort.by("_id").descending()).limit(1);
			ApiCallMinuteStatsDto lastApiCallMinuteStatsDto = apiCallMinuteStatsService.findOne(apiCallStatsQuery);
			String lastApiCallId = null;
			if (null != lastApiCallMinuteStatsDto) {
				lastApiCallId = StringUtils.isBlank(lastApiCallMinuteStatsDto.getLastApiCallId()) ? null : lastApiCallMinuteStatsDto.getLastApiCallId();
			}
			if (log.isDebugEnabled()) {
				log.debug(" {} - Found exists ApiCallStatsDto based on filter: {}, lastApiCallId: {}, exists ApiCallStatsDto: {}", traverseStep, apiCallStatsQuery.getQueryObject().toJson(), lastApiCallId, lastApiCallMinuteStatsDto);
			}

			// Aggregate the ApiCall data of the current module, and wrap new ApiCallMinuteStats
			List<ApiCallMinuteStatsDto> apiCallMinuteStatsDtoList;
			try {
				apiCallMinuteStatsDtoList = apiCallService.aggregateMinuteByAllPathId(moduleId, lastApiCallId, Date.from(startInstant));
			} catch (Exception e) {
				log.error("Aggregate ApiCallStatsDto failed, moduleId: {}, will skip it, error: {}", moduleId, e.getMessage(), e);
				continue;
			}

			// Merge the new ApiCallMinuteStats with the historical ApiCallMinuteStats
			try {
				apiCallMinuteStatsService.merge(apiCallMinuteStatsDtoList);
			} catch (Exception e) {
				log.error("Merge ApiCallStatsDto failed, will skip it, error: {}", e.getMessage(), e);
				continue;
			}

			// Save the merged ApiCallMinuteStats
			apiCallMinuteStatsDtoList.forEach(dto -> {
				dto.setUserId(modulesDto.getUserId());
				dto.setLastUpdAt(new Date());
				if (null == dto.getId()) {
					dto.setId(new ObjectId());
					dto.setCreateAt(new Date());
				}
			});
			try {
				apiCallMinuteStatsService.bulkWrite(apiCallMinuteStatsDtoList, ApiCallMinuteStatsEntity.class, entity -> {
					Criteria criteria = Criteria.where("id").is(entity.getId());
					return Query.query(criteria);
				});
				long loopCost = System.currentTimeMillis() - loopStartMs;
				if (log.isDebugEnabled()) {
					StringBuilder sb = new StringBuilder();
					for (ApiCallMinuteStatsDto callMinuteStatsDto : apiCallMinuteStatsDtoList) {
						sb.append(callMinuteStatsDto.toString()).append(System.lineSeparator());
					}
					log.info("Bulk write Api Call Minute Stats data completed, moduleId: {}, cost: {} ms, progress: {}/{}, data: {}", moduleId, loopCost, traverseStep, modulesList.size(), sb);
				}
			} catch (Exception e) {
				log.error("BulkWrite ApiCallMinuteStatsDto failed, will skip it, error: {}", e.getMessage(), e);
			}
		}
	}

	@Scheduled(cron = "0/30 * * * * ?")
	@SchedulerLock(name = "api_call_worker_minute_stats_scheduler", lockAtMostFor = "30m", lockAtLeastFor = "5s")
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
