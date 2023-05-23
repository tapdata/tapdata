package com.tapdata.tm.inspect.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateUtil;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.alarm.constant.AlarmComponentEnum;
import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.alarm.constant.AlarmTypeEnum;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingVO;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.inspect.bean.Source;
import com.tapdata.tm.inspect.bean.Stats;
import com.tapdata.tm.inspect.bean.Task;
import com.tapdata.tm.inspect.constant.InspectResultEnum;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.dto.InspectResultDto;
import com.tapdata.tm.inspect.entity.InspectResultEntity;
import com.tapdata.tm.inspect.param.SaveInspectResultParam;
import com.tapdata.tm.inspect.repository.InspectResultRepository;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.utils.MongoUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2021/09/14
 * @Description:
 */
@Service
@Slf4j
public class InspectResultService extends BaseService<InspectResultDto, InspectResultEntity, ObjectId, InspectResultRepository> {
    @Autowired
    private InspectService inspectService;

    @Autowired
    private DataSourceService dataSourceService;

		@Autowired
		private AlarmService alarmService;

    public InspectResultService(@NonNull InspectResultRepository repository) {
        super(repository, InspectResultDto.class, InspectResultEntity.class);
    }

    protected void beforeSave(InspectResultDto inspectResult, UserDetail user) {
        inspectResult.setTtlTime(InspectDetailsService.buildTtlTime());
    }

    public Page<InspectResultDto> find(Filter filter, UserDetail userDetail, boolean inspectGroupByFirstCheckId) {
        filter.setOrder("createTime desc");
        Page<InspectResultDto> page = find(filter,  userDetail);
        getLastResult(page.getItems(), inspectGroupByFirstCheckId);
        joinResult(page.getItems());
        fillInspectInfo(page.getItems());
        return page;
    }


    public void joinResult(List<InspectResultDto> inspectResultDtos) {
        if (CollectionUtils.isNotEmpty(inspectResultDtos)) {
            for (InspectResultDto inspectResultDto : inspectResultDtos) {
                InspectDto inspectDto;
                if (inspectResultDto.getInspect() != null) {
                    inspectDto = inspectResultDto.getInspect();
                } else {
                    Criteria criteria = Criteria.where("id").is(inspectResultDto.getInspect_id());
                    inspectDto = inspectService.findOne(new Query(criteria));
                }
                Pair<Integer, String> pair = InspectService.joinResult(inspectDto, inspectResultDto);
                inspectResultDto.setDifferenceNumber(pair.getKey());
                inspectResultDto.setResult(pair.getValue());
            }
        }
    }

    private void getLastResult(List<InspectResultDto> inspectResultDtos, boolean inspectGroupByFirstCheckId) {
        List<InspectResultDto> lastResult = new ArrayList<>();
        if (inspectGroupByFirstCheckId) {
            for (int i = 0; i < inspectResultDtos.size(); i++) {
                InspectResultDto inspectResultDto = inspectResultDtos.get(i);
                String firstCheckId = inspectResultDto.getFirstCheckId();
                if (StringUtils.isNotBlank(firstCheckId)) {
                    //nodejs上面写的是like，应该是loopback的处理导致只能用regEx,这边先使用eq
                    Criteria criteria = Criteria.where("firstCheckId").is(firstCheckId);
                    Query query = new Query(criteria);
                    query.with(Sort.by(Sort.Direction.DESC, "id"));
                    InspectResultDto one = findOne(new Query(criteria),"inspect.timing");
                    inspectResultDto.setSourceTotal(one.getFirstSourceTotal());
                    inspectResultDto.setTargetTotal(one.getFirstTargetTotal());
                }
            }
        }
    }


    public void fillInspectInfo(List<InspectResultDto> inspectResultDtos) {
        if (CollectionUtils.isEmpty(inspectResultDtos)) {
            return;
        }

        Set<ObjectId> connIds = new HashSet<>();
        for (InspectResultDto result : inspectResultDtos) {
            InspectDto inspect = result.getInspect();
            if (inspect == null) {
                continue;
            }

            List<Task> tasks = inspect.getTasks();
            if (CollectionUtils.isEmpty(tasks)) {
                continue;
            }

            for (Task task : tasks) {
                if (task == null) {
                    continue;
                }

                if (task.getSource() != null && StringUtils.isNotBlank(task.getSource().getConnectionId())) {
                    connIds.add(MongoUtils.toObjectId(task.getSource().getConnectionId()));
                }

                if (task.getTarget() != null && StringUtils.isNotBlank(task.getTarget().getConnectionId())) {
                    connIds.add(MongoUtils.toObjectId(task.getTarget().getConnectionId()));
                }

            }
        }

        if (connIds.size() == 0) {
            return;
        }

        Criteria criteria = Criteria.where("id").in(connIds);
        Query query = new Query(criteria);
        query.fields().include("id", "name");
        List<DataSourceConnectionDto> connectionDtos = dataSourceService.findAll(query);
        if (CollectionUtils.isEmpty(connectionDtos)) {
            return;
        }
        Map<String, DataSourceConnectionDto> connectionMap = connectionDtos.stream().collect(Collectors.toMap(c -> c.getId().toHexString(), c -> c));

        if (connectionMap.size() == 0) {
            return;
        }

        for (InspectResultDto result : inspectResultDtos) {
            InspectDto inspect = result.getInspect();
            if (inspect != null) {
                List<Task> tasks = inspect.getTasks();
                if (CollectionUtils.isEmpty(tasks)) {
                    continue;
                }

                for (Task task : tasks) {
                    if (task == null) {
                        continue;
                    }
                    Source source = task.getSource();
                    setSourceConnectName(source, connectionMap);

                    Source target = task.getTarget();
                    setSourceConnectName(target, connectionMap);

                }
            }

            List<Stats> stats = result.getStats();
            if (CollectionUtils.isEmpty(stats)) {
                continue;
            }

            for (Stats stat : stats) {
                if (stat == null) {
                    continue;
                }

                Source source = stat.getSource();
                setSourceConnectName(source, connectionMap);

                Source target = stat.getTarget();
                setSourceConnectName(target, connectionMap);

            }
        }

    }

    public void setSourceConnectName(Source source, Map<String, DataSourceConnectionDto> connectionMap) {
        if (source != null && StringUtils.isNotBlank(source.getConnectionId())) {
            DataSourceConnectionDto dataSourceConnectionDto = connectionMap.get(source.getConnectionId());
            if (dataSourceConnectionDto != null) {
                source.setConnectionName(dataSourceConnectionDto.getName());
            }
        }
    }

    public InspectResultDto saveInspectResult(SaveInspectResultParam saveInspectResultParam, UserDetail userDetail) {
        //timing 没有必要报错到结果中
        saveInspectResultParam.getInspect().remove("timing");
        InspectResultDto inspectResultDto = BeanUtil.copyProperties(saveInspectResultParam, InspectResultDto.class);
        super.save(inspectResultDto, userDetail);
        return inspectResultDto;
    }


    public InspectResultDto getLatestInspectResult(ObjectId inspectId) {
        Query query = Query.query(Criteria.where("inspect_id").is(inspectId.toString()));
        query.fields().exclude("inspect.timing");
        query.with(Sort.by("createTime").descending());
        InspectResultEntity inspectResultEntity = repository.getMongoOperations().findOne(query, InspectResultEntity.class);
        InspectResultDto retDto=BeanUtil.copyProperties(inspectResultEntity,InspectResultDto.class);
        return retDto;
    }


    /**
     * 为适应前端，把firstSourceTotal  set到sourceTotal的值，
     * 所以实现该方法
     *
     * @param filter
     * @param userDetail
     * @return
     */
    public InspectResultDto findById(Filter filter, UserDetail userDetail) {
        InspectResultDto inspectResultDto = findOne(filter, userDetail);
        if (null != inspectResultDto) {
            inspectResultDto.setSourceTotal(inspectResultDto.getFirstSourceTotal());
        } else {
            log.error("找不到到校验结果  filter:  {}", JsonUtil.toJson(filter));
        }
        return inspectResultDto;
    }

    /**
     * @param where
     * @param saveInspectResultParam
     * @param userDetail
     * @return
     */
    public InspectResultDto upsertInspectResultByWhere(Where where, SaveInspectResultParam saveInspectResultParam, UserDetail userDetail) {
        InspectResultDto inspectResultDto = BeanUtil.copyProperties(saveInspectResultParam, InspectResultDto.class);

				handlerStatusDoSomething(inspectResultDto, userDetail);
        List<Stats> inspectResultStats = inspectResultDto.getStats() != null ? inspectResultDto.getStats() :
                Collections.emptyList();

        log.info("Update {} of inspect result stats", saveInspectResultParam.isPartStats() ? "part" : "all");

        InspectResultDto resultDto = null;
        if (saveInspectResultParam.isPartStats()) {

            inspectResultDto.setStats(null);

            resultDto = super.upsertByWhere(where, inspectResultDto, userDetail);

            if (inspectResultStats.size() > 0) {
                List<String> taskIds = inspectResultStats.stream().map(Stats::getTaskId).collect(Collectors.toList());
                Update update = new Update().pull("stats", Criteria.where("taskId").in(taskIds).getCriteriaObject());
                UpdateResult result = updateById(resultDto.getId(), update, userDetail);
                if (log.isDebugEnabled()) {
                    log.debug("Pull inspect result stats by task id: {}, result: {}", update.toString(), result);
                }

                update = new Update().push("stats").each(inspectResultStats);
                result = updateById(resultDto.getId(), update, userDetail);
                if (log.isDebugEnabled()) {
                    log.debug("Push inspect result stats {}", result);
                }
            }
            resultDto = super.findById(resultDto.getId());

            AtomicLong sourceTotal = new AtomicLong(0);
            AtomicLong targetTotal = new AtomicLong(0);
            resultDto.getStats().forEach(stats -> {
                sourceTotal.addAndGet(stats.getSourceTotal());
                targetTotal.addAndGet(stats.getTargetTotal());
            });
            UpdateResult result = repository.update(Query.query(Criteria.where("_id").is(resultDto.getId())),
                    Update.update("sourceTotal", sourceTotal.get()).set("targetTotal", targetTotal.get())
                            .set("firstSourceTotal", sourceTotal.get()).set("firstTargetTotal", targetTotal.get())
            );
            log.debug("Update inspect result total statistics for source and target {}, result: {}",
                    resultDto.getId(), result);
        } else {
            resultDto = super.upsertByWhere(where, inspectResultDto, userDetail);
        }


        //需要把结果更新到inspect表
        String inspectId = resultDto.getInspect_id();
        List<Stats> statsList = inspectResultDto.getStats();
        if (CollectionUtils.isNotEmpty(statsList)) {
            //如果stats中包含有failed的，就是校验结果失败，否就是passed
            Boolean passed = statsList.stream().allMatch(
                    a -> (StringUtils.isNotEmpty(a.getResult()) && InspectResultEnum.PASSED.getValue().equals(a.getResult())));
            Update update = new Update();
            if (passed) {
                update.set("result", InspectStatusEnum.PASSED.getValue());
            } else {
                update.set("result", InspectStatusEnum.FAILED.getValue());
            }
            inspectService.updateById(inspectId, update, userDetail);
        }
        return resultDto;
    }

	private void handlerStatusDoSomething(InspectResultDto inspectResultDto, UserDetail userDetail) {

		try {
			if (!InspectResultDto.STATUS_DONE.equals(inspectResultDto.getStatus())) {
				log.info("ignore process status: {}", inspectResultDto.getStatus());
			}
			List<Stats> inspectResultStats = inspectResultDto.getStats();
			if (CollectionUtils.isEmpty(inspectResultStats)) {
				return;
			}
			InspectDto inspectDto = inspectService.findById(MongoUtils.toObjectId(inspectResultDto.getInspect_id()));
			if (inspectDto == null) {
				log.warn("inspect is null");
				return;
			}
			String inspectMethod = inspectDto.getInspectMethod();
			List<AlarmSettingVO> alarmSettings = inspectDto.getAlarmSettings();
			AlarmKeyEnum alarmKeyEnum;
			Function<Stats, AlarmInfo> alarmInfoFunction;
			if (StringUtils.equalsAny(inspectMethod, "row_count", "cdcCount")) {
				alarmKeyEnum = AlarmKeyEnum.INSPECT_COUNT_ERROR;
			} else {
				alarmKeyEnum = AlarmKeyEnum.INSPECT_VALUE_ERROR;
			}

			alarmInfoFunction = (stats) -> {
				Optional<AlarmSettingVO> voOptional = alarmSettings.stream()
								.filter(AlarmSettingVO::isOpen)
								.filter(alarmSettingVO -> alarmSettingVO.getKey() == alarmKeyEnum)
								.findFirst();
				if (!voOptional.isPresent()) {
					return null;
				}
				AlarmSettingVO alarmSettingVO = voOptional.get();
				Map<String, Object> params = alarmSettingVO.getParams();
				if (MapUtils.isEmpty(params)) {
					return null;
				}
				Map<String, Object> alarmParams = new HashMap<>();
				alarmParams.put("inspectName", inspectDto.getName());
				if (alarmKeyEnum == AlarmKeyEnum.INSPECT_COUNT_ERROR) {
					Integer maxDifferentialRows = (Integer) params.get("maxDifferentialRows");
					if (maxDifferentialRows == null) {
						maxDifferentialRows = 0;
					}
					long diff0 = Math.abs(stats.getSourceTotal() - stats.getTargetTotal());
					long diff1 = Math.abs(stats.getSourceOnly() - stats.getTargetOnly());
					if (diff0 < diff1) {
						diff0 = diff1;
					}
					if (diff0 >= maxDifferentialRows) {
						alarmParams.put("count", diff0);
						return AlarmInfo.builder().status(AlarmStatusEnum.ING)
										.level(Level.WARNING)
										.component(AlarmComponentEnum.FE)
										.type(AlarmTypeEnum.INSPECT_ALARM)
										.agentId(inspectDto.getAgentId())
										.inspectId(inspectDto.getId().toHexString())
										.name(inspectDto.getName())
										.summary("INSPECT_COUNT_ERROR")
										.metric(AlarmKeyEnum.INSPECT_COUNT_ERROR)
										.param(alarmParams)
										.lastNotifyTime(DateUtil.date())
										.build();
					}
				} else {
					Integer maxDifferentialValues = (Integer) params.get("maxDifferentialValues");
					if (maxDifferentialValues == null) {
						maxDifferentialValues = 0;
					}
					if (stats.getRowFailed() != null && stats.getRowFailed() >= maxDifferentialValues) {
						alarmParams.put("count", stats.getRowFailed());
						return AlarmInfo.builder().status(AlarmStatusEnum.ING)
										.level(Level.WARNING)
										.component(AlarmComponentEnum.FE)
										.type(AlarmTypeEnum.INSPECT_ALARM)
										.agentId(inspectDto.getAgentId())
										.inspectId(inspectDto.getId().toHexString())
										.name(inspectDto.getName())
										.summary("jointField".equals(inspectMethod) ? "INSPECT_VALUE_JOIN_ERROR" : "INSPECT_VALUE_ALL_ERROR")
										.metric(AlarmKeyEnum.INSPECT_VALUE_ERROR)
										.param(alarmParams)
										.lastNotifyTime(DateUtil.date())
										.build();
					}
				}
				return null;
			};

			List<Stats> statsList = inspectResultStats.stream()
							.filter(i -> StringUtils.equals(i.getStatus(), InspectStatusEnum.DONE.getValue()))
							.filter(i ->  "failed".equals(i.getResult()))
							.collect(Collectors.toList());

			if (CollectionUtils.isNotEmpty(statsList)) {
				for (Stats stats : statsList) {
					boolean checkOpen = alarmService.checkOpen(alarmSettings, alarmKeyEnum, null, userDetail);
					if (checkOpen) {
						AlarmInfo alarmInfo = alarmInfoFunction.apply(stats);
						if (alarmInfo != null) {
							alarmInfo.setUserId(inspectDto.getUserId());
							alarmService.save(alarmInfo);
						}
					}
				}
			}
		} catch (Exception e) {
			log.error("handlerStatusDoSomething error", e);
		}
	}

	public void createAndPatch(InspectResultDto result) {
        if (!InspectResultDto.STATUS_ERROR.equals(result.getStatus()) && !InspectResultDto.STATUS_DONE.equals(result.getStatus())) {
            return;
        }

        String inspectResultResult;

        if (StringUtils.isNotBlank(result.getStatus())) {
            Criteria criteria = Criteria.where("_id").is(MongoUtils.toObjectId(result.getInspect_id()));
            Query query = new Query(criteria);
            InspectDto inspect = inspectService.findOne(query);

            if (InspectResultDto.STATUS_ERROR.equals(result.getStatus())) {
                inspectResultResult = InspectResultDto.STATUS_ERROR;
            } else {
                Pair<Integer, String> pair = InspectService.joinResult(inspect, result);
                if (Objects.equals(result.getTargetTotal(), result.getSourceTotal()) && pair.getKey() == 0) {
                    inspectResultResult = InspectResultDto.RESULT_PASSED;
                } else {
                    inspectResultResult = InspectResultDto.RESULT_FAILED;
                }
            }

            inspectService.update(query, Update.update("result", inspectResultResult));
        }

        if (InspectResultDto.STATUS_ERROR.equals(result.getStatus())) {
            Criteria criteria = Criteria.where("_id").is(MongoUtils.toObjectId(result.getInspect_id()));
            Query query = new Query(criteria);
            InspectDto inspect = inspectService.findOne(query);

        } else if (InspectResultDto.STATUS_DONE.equals(result.getStatus())) {
            List<Stats> stats = result.getStats();
            if (CollectionUtils.isEmpty(stats)) {
                return;
            }
            Criteria criteria = Criteria.where("_id").is(MongoUtils.toObjectId(result.getInspect_id()));
            Query query = new Query(criteria);
            InspectDto inspect = inspectService.findOne(query);

            for (Stats stat : stats) {
                if (InspectResultDto.RESULT_FAILED.equals(stat.getResult())) {
                    if ("row_count".equals(inspect.getInspectMethod())) {
                    } else {
                        Long rowFailed = stats.stream().map(r -> (r.getSourceOnly() == null ? 0 : r.getSourceOnly()) + (r.getTargetOnly() == null ? 0 : r.getTargetOnly())
                                + (r.getRowFailed() == null ? 0 : r.getRowFailed())).reduce(0L, Long::sum);
                    }
                }

            }
        }
    }
}
