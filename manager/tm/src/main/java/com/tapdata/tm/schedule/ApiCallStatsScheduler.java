package com.tapdata.tm.schedule;

import com.tapdata.tm.apiCalls.service.ApiCallService;
import com.tapdata.tm.apiCalls.service.WorkerCallService;
import com.tapdata.tm.apicallstats.dto.ApiCallStatsDto;
import com.tapdata.tm.apicallstats.service.ApiCallStatsService;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2024-08-27 10:30
 **/
@Component
@Slf4j
public class ApiCallStatsScheduler {

	private final ModulesService modulesService;
	private final ApiCallStatsService apiCallStatsService;
	private final ApiCallService apiCallService;
	private final WorkerCallService workerCallService;

	@Autowired
	public ApiCallStatsScheduler(ModulesService modulesService, ApiCallStatsService apiCallStatsService, ApiCallService apiCallService, WorkerCallService workerCallService) {
		this.modulesService = modulesService;
		this.apiCallStatsService = apiCallStatsService;
		this.apiCallService = apiCallService;
		this.workerCallService = workerCallService;
	}

	/**
	 * Scheduled task to aggregate the API call data of each module and save it to ApiCallStats
	 */
	@Scheduled(cron = "0 0/5 * * * ?")
	@SchedulerLock(name = "api_call_stats_scheduler", lockAtMostFor = "30m", lockAtLeastFor = "5s")
	public void schedule() {
		Thread.currentThread().setName(getClass().getSimpleName() + "-scheduler");
		if (log.isDebugEnabled()) {
			log.debug("Start to aggregate ApiCallStats...");
		}
		long startMs = System.currentTimeMillis();

		// Get all Modules, excluding deleted ones
		Query modulesQuery = new Query();
		modulesQuery.fields().include("id", "user_id", "is_deleted");
		List<ModulesDto> modulesList = modulesService.findAll(modulesQuery);
		if (log.isDebugEnabled()) {
			log.debug("Found all modules size: {}, include fields: {}", modulesList.size(), modulesQuery.getFieldsObject().toJson());
		}
		boolean apiCallStatsServiceEmpty = apiCallStatsService.isEmpty();
		if (apiCallStatsServiceEmpty && !modulesList.isEmpty()) {
			log.info("Initializing Api Call Stats data for the first time, discover the number of apis: {}, please wait...", modulesList.size());
		}

		if (CollectionUtils.isEmpty(modulesList)) {
			return;
		}

		// Traverse all Modules, perform pre-aggregation, and save to ApiCallMinuteStats
		int traverseStep = 0;
		for (ModulesDto modulesDto : modulesList) {
			long loopStartMs = System.currentTimeMillis();
			ObjectId moduleOid = modulesDto.getId();
			if (null == moduleOid) {
				continue;
			}
			String moduleId = moduleOid.toString();
			traverseStep++;
			Boolean isDeleted = modulesDto.getIsDeleted();

			if (Boolean.TRUE.equals(isDeleted)) {
				// Delete ApiCallStats which Api is deleted
				apiCallStatsService.deleteAllByModuleId(moduleId);
				long loopCost = System.currentTimeMillis() - loopStartMs;
				if (apiCallStatsServiceEmpty) {
					log.info("Delete Api Call Stats by module id completed, module id: {}, cost: {} ms, progress: {}/{}", modulesDto, loopCost, traverseStep, modulesList.size());
				}
			} else {
				// Get the historical ApiCallStats record based on moduleId, and get the lastApiCallId from it as this offset
				Query apiCallStatsQuery = Query.query(Criteria.where("moduleId").is(moduleId).and("type").nin(1, 2)).limit(1);
				ApiCallStatsDto apiCallStatsDto = apiCallStatsService.findOne(apiCallStatsQuery);
				String lastApiCallId = null;
				if (null != apiCallStatsDto) {
					lastApiCallId = StringUtils.isBlank(apiCallStatsDto.getLastApiCallId()) ? null : apiCallStatsDto.getLastApiCallId();
				}
				if (log.isDebugEnabled()) {
					log.debug(" {} - Found exists ApiCallStatsDto based on filter: {}, lastApiCallId: {}, exists ApiCallStatsDto: {}", traverseStep, apiCallStatsQuery.getQueryObject().toJson(), lastApiCallId, apiCallStatsDto);
				}

				// Aggregate the ApiCall data of the current module, and wrap new ApiCallStats
				ApiCallStatsDto newApiCallStatsDto;
				try {
					newApiCallStatsDto = apiCallService.aggregateByAllPathId(moduleId, lastApiCallId);
				} catch (Exception e) {
					log.error("Aggregate ApiCallStatsDto failed, moduleId: {}, will skip it, error: {}", moduleId, e.getMessage(), e);
					continue;
				}

				// Merge the new ApiCallStats with the historical ApiCallStats
				try {
					apiCallStatsService.merge(apiCallStatsDto, newApiCallStatsDto);
				} catch (Exception e) {
					log.error("Merge ApiCallStatsDto failed, old ApiCallStatsDto: {}, new ApiCallStatsDto: {}, will skip it, error: {}", apiCallStatsDto, newApiCallStatsDto, e.getMessage(), e);
					continue;
				}

				List<ApiCallStatsDto> olds = new ArrayList<>();
				final Map<String, ApiCallStatsDto> oldMap = new HashMap<>();
				if (null != lastApiCallId) {
					Criteria nin = Criteria.where("moduleId").is(moduleId)
							.and("type").nin(1, 2);
					olds = apiCallStatsService.findAll(Query.query(nin));
					if (!olds.isEmpty()) {
						lastApiCallId = olds.get(0).getLastApiCallId();
						oldMap.putAll(olds.stream()
								.filter(Objects::nonNull)
								.collect(Collectors.toMap(
										e -> String.format("%s_%s_%s", e.getProcessId(), e.getWorkerOid(), e.getModuleId()),
										e -> e,
										(e1, e2) -> e2
								)));
					}
				}
				// Aggregate the ApiCall data of the current module, and wrap new ApiCallStats
				List<ApiCallStatsDto> aggregateWithFacet = workerCallService.aggregateWithFacet(lastApiCallId, newApiCallStatsDto.getLastApiCallId());
				aggregateWithFacet.stream().collect(Collectors.groupingBy(
						ApiCallStatsDto::getProcessId, Collectors.collectingAndThen(Collectors.toList(), list -> {
							ApiCallStatsDto processDto = new ApiCallStatsDto();
							processDto.setProcessId(list.get(0).getProcessId());
							processDto.setModuleId(moduleId);
							processDto.setType(1);
							list.forEach(e -> apiCallStatsService.merge(e, processDto));
							processDto.setId(new ObjectId());
							return processDto;
						}))).forEach((processId, callStatsDto) -> {
					String key = String.format("%s_%s_%s", callStatsDto.getProcessId(), callStatsDto.getWorkerOid(), null);
					ApiCallStatsDto old = oldMap.get(key);
					apiCallStatsService.merge(old, callStatsDto);
					upsert(
							callStatsDto,
							Query.query(Criteria.where("moduleId").is(moduleId).and("type").is(2)),
							apiCallStatsServiceEmpty,
							modulesDto.getUserId(),
							loopStartMs,
							-1,
							modulesList
					);
				});
				for (ApiCallStatsDto callStatsDto : aggregateWithFacet) {
					String key = String.format("%s_%s_%s", callStatsDto.getProcessId(), callStatsDto.getWorkerOid(), callStatsDto.getModuleId());
					ApiCallStatsDto old = oldMap.get(key);
					apiCallStatsService.merge(old, callStatsDto);
					upsert(
						callStatsDto,
						Query.query(Criteria.where("moduleId").is(moduleId).and("type").is(1)),
						apiCallStatsServiceEmpty,
						modulesDto.getUserId(),
						loopStartMs,
						traverseStep,
						modulesList
					);
				}
				upsert(
					newApiCallStatsDto,
					Query.query(Criteria.where("moduleId").is(moduleId).and("type").nin(1, 2)),
					apiCallStatsServiceEmpty,
					modulesDto.getUserId(),
					loopStartMs,
					traverseStep,
					modulesList
				);
			}
		}
		long cost = System.currentTimeMillis() - startMs;
		if (apiCallStatsServiceEmpty && !modulesList.isEmpty()) {
			log.info("Initialize Api Call Stats data for the first time completed, cost: {} ms", cost);
		}
	}

	void upsert(ApiCallStatsDto newApiCallStatsDto, Query upsertQuery, boolean apiCallStatsServiceEmpty, String userId, long loopStartMs, int traverseStep, List<ModulesDto> modulesList) {
		// Calculate accessFailureRate
		if (newApiCallStatsDto.getCallTotalCount() > 0) {
			double rate = new BigDecimal(newApiCallStatsDto.getCallAlarmTotalCount()).divide(new BigDecimal(newApiCallStatsDto.getCallTotalCount()), 4, RoundingMode.HALF_UP).doubleValue();
			newApiCallStatsDto.setAccessFailureRate(rate);
		}

		// Save the merged ApiCallStats
		try {
			newApiCallStatsDto.setLastUpdAt(new Date());
			newApiCallStatsDto.setUserId(userId);
			apiCallStatsService.upsert(upsertQuery, newApiCallStatsDto);
			long loopCost = System.currentTimeMillis() - loopStartMs;
			if (apiCallStatsServiceEmpty) {
				log.info("Upsert one Api Call Stats completed, filter: {}, cost: {} ms, progress: {}/{}, stats data: {}", upsertQuery.getQueryObject().toJson(), loopCost, traverseStep, modulesList.size(), newApiCallStatsDto);
			}
			if (log.isDebugEnabled()) {
				log.debug("Upsert one Api Call Stats completed, filter: {}, cost: {} ms, progress: {}/{}, stats data: {}", upsertQuery.getQueryObject().toJson(), loopCost, traverseStep, modulesList.size(), newApiCallStatsDto);
			}
		} catch (Exception e) {
			log.error("Upsert one Api Call Stats failed, query: {}, data: {}, will skip it, error: {}",
					null != upsertQuery ? upsertQuery.getQueryObject().toJson() : "null", newApiCallStatsDto, e.getMessage(), e);
		}
	}
}
