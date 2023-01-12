/**
 * @title: DataFlowService
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.behavior.BehaviorCode;
import com.tapdata.tm.behavior.service.BehaviorService;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataService;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.SchemaTransformerResult;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.vo.TableOperation;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.websocket.AllowRemoteCall;
import com.tapdata.tm.commons.websocket.MessageInfoBuilder;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dag.util.DAGUtils;
import com.tapdata.tm.dataflow.StartType;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflow.dto.DataFlowResetAllReqDto;
import com.tapdata.tm.dataflow.dto.DataFlowResetAllResDto;
import com.tapdata.tm.dataflow.dto.Stage;
import com.tapdata.tm.dataflow.entity.DataFlow;
import com.tapdata.tm.dataflow.repository.DataFlowRepository;
import com.tapdata.tm.dataflowinsight.service.DataFlowInsightService;
import com.tapdata.tm.dataflowrecord.service.DataFlowRecordService;
import com.tapdata.tm.dataflowsdebug.service.DataFlowsDebugService;
import com.tapdata.tm.commons.schema.bean.PlatformInfo;
import com.tapdata.tm.inspect.entity.InspectEntity;
import com.tapdata.tm.inspect.service.InspectDetailsService;
import com.tapdata.tm.inspect.service.InspectResultService;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.job.service.JobService;
import com.tapdata.tm.jobddlhistories.service.JobDDLHistoriesService;
import com.tapdata.tm.log.service.LogService;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.statemachine.StateMachine;
import com.tapdata.tm.statemachine.constant.StateMachineConstant;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.enums.DataFlowState;
import com.tapdata.tm.statemachine.model.DataFlowStateTrigger;
import com.tapdata.tm.typemappings.service.DataTypeSupportService;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.utils.UUIDUtil;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.util.CloseableIterator;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

@Component
@Slf4j
public class DataFlowService extends BaseService<DataFlowDto, DataFlow, ObjectId, DataFlowRepository> {

	@Autowired
	private StateMachine<DataFlowState, DataFlowEvent> stateMachine;

	private final UserLogService userLogService;

	private final JobService jobService;

	@Autowired
	private DataFlowsDebugService dataFlowsDebugService;

	@Autowired
	private DataFlowInsightService dataFlowInsightService;

	@Autowired
	private MetadataInstancesService metadataInstancesService;

	private final JobDDLHistoriesService jobDDLHistoriesService;

	@Autowired
	private InspectService inspectService;

	@Autowired
	private InspectResultService inspectResultService;

	private final InspectDetailsService inspectDetailsService;

	@Autowired
	private WorkerService workerService;

	@Autowired
	private DAGDataService dagDataService;

	@Autowired
	private DataFlowRecordService dataFlowRecordService;

	@Autowired
	private DataTypeSupportService dataTypeSupportService;

	@Autowired
	private MessageQueueService messageQueueService;

	@Autowired
	private BehaviorService behaviorService;

	@Autowired
	private LogService logService;

	private static ThreadPoolExecutor completableFutureThreadPool;

	@Value("${tm.transform.batch.num:20}")
	private int transformBatchNum;

	private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	static {
		int poolSize = Runtime.getRuntime().availableProcessors() * 2;
		completableFutureThreadPool = new ThreadPoolExecutor(poolSize, poolSize,
				0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
	}

	public DataFlowService(DataFlowRepository repository,
	                       UserLogService userLogService, JobService jobService,
	                       JobDDLHistoriesService jobDDLHistoriesService,
	                       InspectDetailsService inspectDetailsService) {
		super(repository, DataFlowDto.class, DataFlow.class);
		this.userLogService = userLogService;
		this.jobService = jobService;
		this.jobDDLHistoriesService = jobDDLHistoriesService;
		this.inspectDetailsService = inspectDetailsService;
	}

	@Override
	protected void beforeSave(DataFlowDto dto, UserDetail userDetail) {

		List<String> tags = new ArrayList<>();
		if (dto.getPlatformInfo() != null) {
			PlatformInfo platformInfo = dto.getPlatformInfo();
			if (StringUtils.isNotBlank(platformInfo.getRegion())) {
				tags.add(platformInfo.getRegion());
			}

			if (StringUtils.isNotBlank(platformInfo.getZone())) {
				tags.add(platformInfo.getZone());
			}

			if (StringUtils.isNotBlank(platformInfo.getAgentType())) {
				tags.add(platformInfo.getAgentType());
			}

			if (platformInfo.getIsThrough() != null && platformInfo.getIsThrough()) {
				tags.add("internet");
			}

			if (platformInfo.getBidirectional() != null && platformInfo.getBidirectional()) {
				tags.add("bidirectional");
			}
		}

		if (CollectionUtils.isNotEmpty(tags)){
			dto.setAgentTags(tags);
		}
	}

	@Override
	public Page<DataFlowDto> find(Filter filter, UserDetail userDetail) {
		Page<DataFlowDto> dataFlowDtoPage = super.find(filter, userDetail);
		List<DataFlowDto> items = dataFlowDtoPage.getItems();
		if (CollectionUtils.isNotEmpty(items)){
			List<String> statusList = Arrays.asList("running", "scheduled", "stopping", "force stopping");
			Set<String> processIds = items.stream().filter(item -> item != null
					&& statusList.contains(item.getStatus()))
					.map(DataFlowDto::getAgentId).collect(Collectors.toSet());
			List<Worker> workers = workerService.findAll(Query.query(Criteria.where("process_id").in(processIds)), userDetail);
			items.stream()
					.filter(dataFlowDto -> dataFlowDto != null && statusList.contains(dataFlowDto.getStatus()))
					.forEach(dataFlowDto -> workers.stream()
							.filter(worker -> worker != null && dataFlowDto.getAgentId().equals(worker.getProcessId()))
							.findFirst().ifPresent(worker -> dataFlowDto.setTcm(worker.getTcmInfo())));
		}
		return dataFlowDtoPage;
	}

	public long updateById(String id, DataFlowDto dto, UserDetail userDetail) {
		DataFlowDto dataFlowDto = findOne(Query.query(Criteria.where("_id").is(new ObjectId(id))), userDetail);
		if (dataFlowDto == null){
			throw new BizException("DataFlow.Not.Found");
		}
		dto.setId(dataFlowDto.getId());
		save(dto, userDetail);

		behaviorService.trace(dto, userDetail, BehaviorCode.editDataFlow);

		return 0;
	}

	/**
	 *  云版任务启动调用该方法
	 * @param dto
	 * @param userDetail
	 * @return
	 */
	public Map<String, Object> patch(Map<String, Object> dto, UserDetail userDetail) {

		if (MapUtils.isEmpty(dto)){
			throw new BizException("IllegalArgument", "dataFlow");
		}
		String id = MapUtils.getAsString(dto, "id");
		if (StringUtils.isBlank(id)){
			throw new BizException("IllegalArgument", "id");
		}
		DataFlowDto dataFlowDto = findById(toObjectId(id), userDetail);
		if (dataFlowDto == null){
			throw new BizException("DataFlow.Not.Found");
		}
		String status = MapUtils.getAsString(dto, "status");
		if (StateMachineConstant.DATAFLOW_STATUS_FORCE_STOPPING.equals(status)){
			updateById(toObjectId(id), Update.update("status", status).set("forceStoppingTime", new Date()), userDetail);

			messageQueueService.sendMessage(dataFlowDto.getAgentId(),
					MessageInfoBuilder.newMessage()
							.call("dataFlowScheduler", "forceStoppingDataFlow")
							.body(dataFlowDto.getId().toHexString()).build());

			behaviorService.trace(dataFlowDto, userDetail, BehaviorCode.forceStopDataFlow);

			return dto;
		}
		DataFlowStateTrigger trigger = new DataFlowStateTrigger();
		dto.put("user_id", userDetail.getUserId());
		trigger.setData(dataFlowDto);
		trigger.setUserDetail(userDetail);
		DataFlowState state = DataFlowState.getState(dataFlowDto.getStatus());
		trigger.setSource(state);
		switch (status){
			case "scheduled":
				trigger.setEvent(DataFlowEvent.START);
				dataFlowDto.setStartType(StartType.manual.name());
				behaviorService.trace(dataFlowDto, userDetail, BehaviorCode.startDataFlow);
				break;
			case "stopping":
				trigger.setEvent(DataFlowEvent.STOP);
				behaviorService.trace(dataFlowDto, userDetail, BehaviorCode.stopDataFlow);
				break;
			case "paused":
				trigger.setEvent(DataFlowEvent.STOPPED);
				behaviorService.trace(dataFlowDto, userDetail, BehaviorCode.pausedDataFlow);
				break;
			default:
				throw new BizException("Not.Supported");
		}

		stateMachine.handleEvent(trigger);

		return dto;
	}

	public DataFlowDto save(DataFlowDto dto, UserDetail userDetail) {

		if (dto == null){
			throw new BizException("IllegalArgument", "dataFlow");
		}
		String name = dto.getName();
		if (StringUtils.isNotBlank(name)){
			Criteria criteria = Criteria.where("name").is(name);
			if (dto.getId() != null){
				criteria.and("_id").ne(dto.getId());
			}
			long count = count(Query.query(criteria), userDetail);
			if (count > 0){
				throw new BizException("Name.Already.Exists");
			}
		}
		setNextScheduleTime(dto);

		boolean isNew = dto.getId() == null;

		DataFlowDto dataFlowDto = super.save(dto, userDetail);
		List<SchemaTransformerResult> mapping = transformSchema(dataFlowDto, userDetail);
		dataFlowDto.setMetadataMappings(mapping);

		if (isNew) {
			behaviorService.trace(dataFlowDto, userDetail, BehaviorCode.createDataFlow);

			logService.afterCreatedDataFlow(dataFlowDto.getId().toHexString());
		}

		return dataFlowDto;
	}

	@Override
	public boolean deleteById(ObjectId objectId, UserDetail userDetail) {
		boolean result = super.deleteById(objectId, userDetail);

		if (result) {
			logService.afterDeletedDataFlow(objectId.toHexString());
		}
		return result;
	}

	/**
	 * 设置定时调度任务时间
	 **/
	public void setNextScheduleTime(DataFlowDto dto){
		Map<String, Object> setting = dto.getSetting();
		Boolean isSchedule = MapUtils.getAsBoolean(setting, "isSchedule");
		if (isSchedule != null && isSchedule){
			if ("initial_sync".equals(dto.getSetting().get("sync_type"))){
				String cronExpression = MapUtils.getAsString(setting, "cronExpression");
				if (StringUtils.isNotBlank(cronExpression)){
					if (!CronExpression.isValidExpression(cronExpression)){
						throw new BizException("IllegalArgument", "cronExpression");
					}
					LocalDateTime next = CronExpression.parse(cronExpression).next(LocalDateTime.now());
					if (next != null){
						long epochMilli = next.toInstant(ZoneOffset.of("+8")).toEpochMilli();
						dto.setNextScheduledTime(epochMilli);
					}
				}
			}else if ("cdc".equals(dto.getSetting().get("sync_type"))){
				String scheduleTime = MapUtils.getAsString(setting, "scheduleTime");
				if (StringUtils.isNotBlank(scheduleTime)){
					LocalDateTime time = LocalDateTime.parse(scheduleTime, dtf);
					long epochMilli = time.toInstant(ZoneOffset.of("+8")).toEpochMilli();
					dto.setNextScheduledTime(epochMilli);
				}
			}else {
				dto.setNextScheduledTime(null);
			}
		}else {
			dto.setNextScheduledTime(null);
		}
	}

	public UpdateResult updateOne(Query query, Map<String, Object> map){
		return repository.updateOne(query, map);
	}

	public DataFlowDto copyDataFlow(String id, UserDetail userDetail){
		DataFlowDto dataFlowDto = findById(new ObjectId(id), userDetail);
		if (dataFlowDto == null){
			throw new BizException("DataFlow.Not.Found");
		}

		String name = dataFlowDto.getName();
		Query query = Query.query(new Criteria().andOperator(Criteria.where("name").regex("^" + name.replace("(", "\\(").replace(")", "\\)")), Criteria.where("name").ne(name)));
		query.fields().include("name");
		List<DataFlow> dataFlows = findAll(query, userDetail);
		if (CollectionUtils.isEmpty(dataFlows)){
			dataFlowDto.setName(name + " (1)");
		}else {
			List<String> names = dataFlows.stream().map(DataFlow::getName).collect(Collectors.toList());
			IntStream.rangeClosed(1, dataFlows.size() + 1)
					.filter(i -> !names.contains(name + " (" + i + ")"))
					.findFirst()
					.ifPresent(i -> dataFlowDto.setName(name + " (" + i + ")"));
		}

		dataFlowDto.setId(null);
		dataFlowDto.setAgentId(null);
		dataFlowDto.setCreateAt(null);
		dataFlowDto.setStartTime(null);
		dataFlowDto.setScheduledTime(null);
		dataFlowDto.setStoppingTime(null);
		dataFlowDto.setForceStoppingTime(null);
		dataFlowDto.setRunningTime(null);
		dataFlowDto.setErrorTime(null);
		dataFlowDto.setPausedTime(null);
		dataFlowDto.setFinishTime(null);
		dataFlowDto.setValidateBatchId(null);
		dataFlowDto.setValidateStatus(null);
		dataFlowDto.setValidateFailedMSG(null);
		dataFlowDto.setOperationTime(null);
		dataFlowDto.setCdcLastTimes(null);
		dataFlowDto.setMilestones(null);
		dataFlowDto.setStatus("paused");
		dataFlowDto.setExecuteMode("normal");
		dataFlowDto.setStats(null);
		DataFlowDto newDataFlow = save(dataFlowDto, userDetail);

/*		UserLogs userLogs = new UserLogs();
		userLogs.setModular("custom".equals(dataFlowDto.getMappingTemplate()) ? "sync" : "migration");
		userLogs.setOperation("copy");
		userLogs.setParameter1(name);
		userLogs.setUserId(userDetail.getUserId());
		userLogs.setParameter2(dataFlowDto.getName());
		userLogs.setParameter3("");
		userLogs.setSourceId(new ObjectId(id));
		userLogs.setType("userOperation");
		userLogService.add(userLogs);*/

		return newDataFlow;
	}

	public DataFlowResetAllResDto resetDataFlow(DataFlowResetAllReqDto dataFlowResetAllReqDto, UserDetail userDetail){
		if (dataFlowResetAllReqDto == null || CollectionUtils.isEmpty(dataFlowResetAllReqDto.getId())){
			throw new BizException("IllegalArgument", "dataFlow");
		}
		List<String> ids = dataFlowResetAllReqDto.getId();
		List<ObjectId> collect = ids.stream().map(ObjectId::new).collect(Collectors.toList());
		Query query = Query.query(Criteria.where("_id").in(collect));
		query.fields().include("status").include("name");
		List<DataFlow> dataFlowList = findAll(query, userDetail);
		if (CollectionUtils.isEmpty(dataFlowList)){
			throw new BizException("DataFlow.Not.Found");
		}
		List<String> dataFlowIds = new ArrayList<>();
		for (DataFlow dataFlow : dataFlowList) {
			dataFlowIds.add(dataFlow.getId().toHexString());
		}
		DataFlowResetAllResDto dataFlowResetAllResDto = new DataFlowResetAllResDto();
		ids.stream().filter(id -> !dataFlowIds.contains(id))
				.forEach(id -> dataFlowResetAllResDto.addFail(id, "DataFlow does not exist"));

		List<String> removeIds = new ArrayList<>(dataFlowIds);
		List<String> statusList = Arrays.asList("scheduled", "running", "stopping", "force stopping");
		dataFlowList.stream().filter(dataFlow -> statusList.contains(dataFlow.getStatus())).forEach(dataFlow -> {
			dataFlowResetAllResDto.addFail(dataFlow.getId().toHexString(),
					String.format("Status %s cannot be reset", dataFlow.getStatus()));
			removeIds.remove(dataFlow.getId().toHexString());
		});

		if (CollectionUtils.isEmpty(removeIds)){
			return dataFlowResetAllResDto;
		}

		CompletableFuture.allOf(CompletableFuture.runAsync(() -> {
			Update update = Update.update("stats", null)
					.set("executeMode", "normal")
					.set("cdcLastTimes", null)
					.set("milestones", null)
					.set("finishTime", null)
					.set("edgeMilestones", null);
			update(Query.query(Criteria.where("_id").in(removeIds)), update);
		}, completableFutureThreadPool), CompletableFuture.runAsync(() -> {
			jobService.deleteAll(Query.query(Criteria.where("dataFlowId").in(removeIds)));
		}, completableFutureThreadPool), CompletableFuture.runAsync(() -> {
			dataFlowsDebugService.deleteAll(Query.query(Criteria.where("__tapd8.dataFlowId").in(removeIds)));
		}, completableFutureThreadPool), CompletableFuture.runAsync(() -> {
			dataFlowInsightService.deleteAll(Query.query(Criteria.where("dataFlowId").in(removeIds)));
		}, completableFutureThreadPool), CompletableFuture.runAsync(() -> {
			metadataInstancesService.deleteAll(Query.query(new Criteria().orOperator(
					Criteria.where("source._id").in(removeIds),
					Criteria.where("source.dataFlowId").in(removeIds)
			)));
		}, completableFutureThreadPool), CompletableFuture.runAsync(() -> {
			jobDDLHistoriesService.deleteAll(Query.query(Criteria.where("dataFlowId").in(removeIds)));
		}, completableFutureThreadPool)).join();
		removeIds.forEach(dataFlowResetAllResDto::addSuccess);
		return dataFlowResetAllResDto;
	}

	public DataFlowResetAllResDto removeDataFlow(String whereJson, UserDetail userDetail){
		if (StringUtils.isBlank(whereJson)){
			throw new BizException("IllegalArgument", "where");
		}
		Map map = JsonUtil.parseJson(whereJson, Map.class);
		Object value = MapUtils.getValueByPatchPath(map, "_id/inq");
		if (value == null){
			value = MapUtils.getValueByPatchPath(map, "id/inq");
		}
		if (!(value instanceof List) || CollectionUtils.isEmpty((List)value)){
			throw new BizException("IllegalArgument", "dataFlowId");
		}
		List<String> ids = (List<String>) value;
		List<ObjectId> collect = ids.stream().map(ObjectId::new).collect(Collectors.toList());
		Query query = Query.query(Criteria.where("_id").in(collect));
		query.fields().include("status").include("name");
		List<DataFlow> dataFlowList = findAll(query, userDetail);
		if (CollectionUtils.isEmpty(dataFlowList)){
			throw new BizException("DataFlow.Not.Found");
		}
		List<String> dataFlowIds = new ArrayList<>();
		for (DataFlow dataFlow : dataFlowList) {
			dataFlowIds.add(dataFlow.getId().toHexString());
		}
		DataFlowResetAllResDto dataFlowResetAllResDto = new DataFlowResetAllResDto();
		ids.stream().filter(id -> !dataFlowIds.contains(id))
				.forEach(id -> dataFlowResetAllResDto.addFail(id, "DataFlow does not exist"));

		List<String> removeIds = new ArrayList<>(dataFlowIds);
		List<String> statusList = Arrays.asList("draft", "paused", "error");
		dataFlowList.stream().filter(dataFlow -> !statusList.contains(dataFlow.getStatus())).forEach(dataFlow -> {
			dataFlowResetAllResDto.addFail(dataFlow.getId().toHexString(),
					String.format("Status %s cannot be remove", dataFlow.getStatus()));
			removeIds.remove(dataFlow.getId().toHexString());
		});

		if (CollectionUtils.isEmpty(removeIds)){
			return dataFlowResetAllResDto;
		}

		CompletableFuture.allOf(CompletableFuture.runAsync(() -> {
			deleteAll(Query.query(Criteria.where("_id").in(removeIds)));
		}, completableFutureThreadPool), CompletableFuture.runAsync(() -> {
			jobService.deleteAll(Query.query(Criteria.where("dataFlowId").in(removeIds)));
		}, completableFutureThreadPool), CompletableFuture.runAsync(() -> {
			dataFlowsDebugService.deleteAll(Query.query(Criteria.where("__tapd8.dataFlowId").in(removeIds)));
		}, completableFutureThreadPool), CompletableFuture.runAsync(() -> {
			dataFlowInsightService.deleteAll(Query.query(Criteria.where("dataFlowId").in(removeIds)));
		}, completableFutureThreadPool), CompletableFuture.runAsync(() -> {
			dataFlowRecordService.deleteAll(Query.query(Criteria.where("dataFlowId").in(removeIds)));
		}, completableFutureThreadPool), CompletableFuture.runAsync(() -> {
			metadataInstancesService.deleteAll(Query.query(new Criteria().orOperator(
					Criteria.where("source._id").in(removeIds),
					Criteria.where("source.dataFlowId").in(removeIds)
			)));
		}, completableFutureThreadPool), CompletableFuture.runAsync(() -> {
			jobDDLHistoriesService.deleteAll(Query.query(Criteria.where("dataFlowId").in(removeIds)));
		}, completableFutureThreadPool)).join();

		List<InspectEntity> inspectList = inspectService.findAll(Query.query(Criteria.where("flowId").in(removeIds)), userDetail);
		if (CollectionUtils.isNotEmpty(inspectList)){
			List<String> inspectIds = new ArrayList<>();
			for (InspectEntity inspectEntity : inspectList) {
				inspectIds.add(inspectEntity.getId().toHexString());
			}
			CompletableFuture.allOf(CompletableFuture.runAsync(() -> {
				inspectService.deleteAll(Query.query(Criteria.where("flowId").in(removeIds)));
			}, completableFutureThreadPool), CompletableFuture.runAsync(() -> {
				inspectResultService.deleteAll(Query.query(Criteria.where("inspect_id").in(inspectIds)));
			}, completableFutureThreadPool), CompletableFuture.runAsync(() -> {
				inspectDetailsService.deleteAll(Query.query(Criteria.where("inspect_id").in(inspectIds)));
			}, completableFutureThreadPool)).join();
		}
		removeIds.forEach(dataFlowResetAllResDto::addSuccess);

		removeIds.forEach(id -> {
			behaviorService.trace(id, userDetail, BehaviorCode.deleteDataFlow);
		});

		return dataFlowResetAllResDto;
	}


	public void chart() {

	}

	/**
	 * 模型推演，为前端数据迁移提供模型推演方法
	 *
	 * TODO: 目前实现为每次推演处理全量表，需要优化为 多线程分批执行，避免 一个库 1 万张表耗光内存
	 * @param dataFlowDto
	 * @return
	 */
	public List<SchemaTransformerResult> transformSchema(DataFlowDto dataFlowDto, UserDetail userDetail) {
		List<Stage> stages = dataFlowDto.getStages().stream().map(s -> {
			//Stage stage = new Stage();
			return JsonUtil.parseJsonUseJackson(JsonUtil.toJson(s), Stage.class);
		}).collect(Collectors.toList());

		DAG dag = DAGUtils.build(userDetail.getUserId(), dataFlowDto.getId(), stages, dagDataService);

		Map<String, List<SchemaTransformerResult>> results = new HashMap<>();

		dag.addNodeEventListener(new Node.EventListener<List<com.tapdata.tm.commons.schema.Schema>>() {
			@Override
			public void onTransfer(List<List<Schema>> inputSchemaList, List<Schema> schema, List<Schema> outputSchema, String nodeId) {
				List<SchemaTransformerResult> schemaTransformerResults = results.get(nodeId);
				if (schemaTransformerResults == null) {
					return;
				}
				List<String> sourceQualifiedNames = outputSchema.stream().map(Schema::getQualifiedName).collect(Collectors.toList());
				Criteria criteria = Criteria.where("qualified_name").in(sourceQualifiedNames);
				Query query = new Query(criteria);
				query.fields().include("_id", "qualified_name");
				List<MetadataInstancesEntity> all = metadataInstancesService.findAll(query, userDetail);
				Map<String, MetadataInstancesEntity> metaMaps = all.stream().collect(Collectors.toMap(MetadataInstancesEntity::getQualifiedName, m -> m, (m1, m2) -> m1));
				for (SchemaTransformerResult schemaTransformerResult : schemaTransformerResults) {
					MetadataInstancesEntity metadataInstancesEntity = metaMaps.get(schemaTransformerResult.getSinkQulifiedName());
					if (metadataInstancesEntity != null && metadataInstancesEntity.getId() != null) {
						schemaTransformerResult.setSinkTableId(metadataInstancesEntity.getId().toHexString());
					}
				}

				// update delete field count
				if (results.containsKey(nodeId)) {
					schemaTransformerResults= results.get(nodeId);
					Function<Schema, Map<String, com.tapdata.tm.commons.schema.Field>> fieldCollector =
							(Schema t) -> t.getFields().stream().collect(Collectors.toMap(
							f -> t.getOriginalName() + f.getFieldName(), f -> f, (f1, f2) -> f1
					));
					Map<String, Map<String, com.tapdata.tm.commons.schema.Field>> fieldMap = outputSchema.parallelStream()
							.collect(Collectors.toMap(
									Schema::getOriginalName,
									fieldCollector,
									(t1, t2) -> t1));

					Map<String, List<String>> invalidFieldMap = outputSchema.parallelStream()
							.collect(Collectors.toMap(Schema::getOriginalName, Schema::getInvalidFields, (t1, t2) -> t1));

					schemaTransformerResults.forEach( schemaTransformerResult -> {
						if (fieldMap.containsKey(schemaTransformerResult.getSinkObjectName())) {
							Map<String, com.tapdata.tm.commons.schema.Field> fieldMaps = fieldMap.get(schemaTransformerResult.getSinkObjectName());
							List<String> invalidFields = invalidFieldMap.get(schemaTransformerResult.getSinkObjectName());
							long deleteCount = fieldMaps.values().stream().filter(com.tapdata.tm.commons.schema.Field::isDeleted).count();
							deleteCount = deleteCount - schemaTransformerResult.getSourceDeletedFieldCount();
							int sinkInvalidFieldCount = invalidFields != null ? invalidFields.size() : 0;

//							schemaTransformerResult.setSinkInvalidFieldCount(sinkInvalidFieldCount);
							schemaTransformerResult.setInvalid( sinkInvalidFieldCount > 0 || schemaTransformerResult.isInvalid());
							schemaTransformerResult.setUserDeletedNum(deleteCount < 0 ? 0 : Long.valueOf(deleteCount).intValue());
						}
					});
				}
			}

			@Override
			public void schemaTransformResult(String nodeId, Node node, List<SchemaTransformerResult> schemaTransformerResults) {
				if (DatabaseNode.SELF_TYPE.equals(node.getType())) {
					DatabaseNode databaseNode = (DatabaseNode) node;
					List<TableOperation> tableOperation = databaseNode.getTableOperations();
					if (("vika".equals(databaseNode.getDatabaseType()) || "qingflow".equals(databaseNode.getDatabaseType())) && tableOperation != null) {
						schemaTransformerResults.forEach(str -> {
							Optional<TableOperation> optional = tableOperation.stream()
									.filter(top -> "rename".equals(top.getType()) && top.getTableName().equals(str.getSinkObjectName()))
									.findFirst();
							if (!optional.isPresent()) {
								str.setInvalid(true);
								str.setSinkObjectName(null);
								str.setSinkQulifiedName(null);
							}
						});
					}
				}
				List<SchemaTransformerResult> results1 = results.get(nodeId);
				if (CollectionUtils.isNotEmpty(results1)) {
					results1.addAll(schemaTransformerResults);
				} else {
					results.put(nodeId, schemaTransformerResults);
				}
			}

			@Override
			public List<SchemaTransformerResult> getSchemaTransformResult(String nodeId) {
				return results.get(nodeId);
			}
		});

		DAG.Options options = new DAG.Options(
				dataFlowDto.getRollback(), dataFlowDto.getRollbackTable(), dataFlowDto.getCustomTypeMappings());
		options.setBatchNum(transformBatchNum);
		if (StringUtils.isBlank(options.getUuid())) {
			options.setUuid(UUIDUtil.getUUID());
		}
		dag.transformSchema(null, dagDataService, options);

		List<SchemaTransformerResult> result = results.values().stream().flatMap(Collection::stream).collect(Collectors.toList());

		List<Node> sourceNodes = dag.getSourceNodes();
		List<String> nodeIds = sourceNodes.stream().map(Node::getId).collect(Collectors.toList());
		List<String> sourceQualifiedNames = result.stream().filter(s -> nodeIds.contains(s.getSourceNodeId()))
				.map(SchemaTransformerResult::getSourceQualifiedName).collect(Collectors.toList());
		Map<String, SchemaTransformerResult> map = result.stream()
				.collect(Collectors.toMap(SchemaTransformerResult::getSourceQualifiedName, s -> s, (s1, s2) -> s1));
		Query query = Query.query(Criteria.where("qualified_name").in(sourceQualifiedNames));
		query.fields().include("fields", "id", "qualified_name", "original_name");

		Map<String, List<String>> deletedFields = metadataInstancesService.findAll(query).stream()
				.peek(m -> {
					SchemaTransformerResult s = map.get(m.getQualifiedName());
					if (s == null)
						return;
					List<Field> unsupportedFields = m.getFields().stream().filter(f ->
							!dataTypeSupportService.supportDataType(s.getSourceDbType(), s.getSinkDbType(), f.getDataType()))
							//.map(Field::getFieldName)
							.collect(Collectors.toList());
					m.setFields(unsupportedFields);
				})
				.filter(m -> m.getFields() != null && m.getFields().size() > 0)
				.collect(Collectors.toMap(MetadataInstancesDto::getOriginalName, m ->
								m.getFields().stream().map(Field::getFieldName).collect(Collectors.toList()),
						(m1, m2) -> m1));

		if (dataFlowDto.getId() != null) {
			UpdateResult a = repository.update(Query.query(Criteria.where("_id").is(dataFlowDto.getId())),
					Update.update("metadataMappings", result).set("deletedFields", deletedFields), userDetail);
		}

		return result;
	}

	/**
	 * Flow engine ping data flow
	 * @param ids data flow id，multiple separated by commas
	 * @return
	 */
	@AllowRemoteCall
	public int pingRunningDataFlow(String ids) {
		return 0;
	}

	public CloseableIterator<DataFlow> stream(Query query) {
		query.cursorBatchSize(1000);
		return repository.getMongoOperations().stream(query, DataFlow.class);
	}
}
