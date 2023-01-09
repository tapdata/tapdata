/**
 * @title: DataFlowController
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.behavior.BehaviorCode;
import com.tapdata.tm.behavior.service.BehaviorService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.commons.dag.SchemaTransformerResult;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.*;
import com.tapdata.tm.dataflow.service.DataFlowService;
import com.tapdata.tm.dataflow.service.DataflowChartService;
import com.tapdata.tm.dataflowrecord.dto.DataFlowRecordDto;
import com.tapdata.tm.dataflowrecord.service.DataFlowRecordService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.statemachine.dto.StateMachineDto;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.typemappings.constant.TypeMappingDirection;
import com.tapdata.tm.typemappings.entity.TypeMappingsEntity;
import com.tapdata.tm.typemappings.service.TypeMappingsService;
import com.tapdata.tm.utils.MongoUtils;
import io.github.openlg.graphlib.Graph;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

@Tag(name = "DataFlow", description = "DataFlow api")
@RestController
@Slf4j
@RequestMapping(value = {"/api/DataFlows", "/api/Dataflows"})
@Setter(onMethod_ = {@Autowired})
public class DataFlowController extends BaseController {

    private DataFlowService dataFlowService;


    @Autowired
    @Lazy
    private TaskService taskService;
    private MessageService messageService;
    private TypeMappingsService typeMappingService;
    private StateMachineService stateMachineService;

    @Autowired
    private DataFlowRecordService dataFlowRecordService;
    @Autowired
    private BehaviorService behaviorService;

    private final Counter dataFlowPing = Counter.builder("data_flow_ping").register(Metrics.globalRegistry);

    @Operation(summary = "Find all dataFlow of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<DataFlowDto>> find(
            @Parameter(in = ParameterIn.QUERY, description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`).")
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        if (filter.getSort() == null) {
            filter.setSort(new ArrayList<>());
        }
        if (filter.getSort().stream().noneMatch(s -> s.startsWith("createAt"))) {
            filter.getSort().add("createAt DESC");
        }
        return success(dataFlowService.find(filter, getLoginUser()));
    }

    @Operation(summary = "Find a dataFlow of the model matched by filter from the data source")
    @GetMapping("/findOne")
    public ResponseMessage<DataFlowDto> findOne(
            @Parameter(in = ParameterIn.QUERY, description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`).")
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        return success(dataFlowService.findOne(filter, getLoginUser()));
    }


    /**
     * 云版任务启动调用该方法
     *
     * @param dto
     * @return
     */
    @Operation(summary = "云版任务启动和停止调用该方法")
    @PatchMapping
    public ResponseMessage<Object> patch(@RequestBody Map<String, Object> dto) {
        return success(dataFlowService.patch(dto, getLoginUser()));
    }

    @Operation(summary = "Create a new dataFlow of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<Object> save(@RequestBody DataFlowDto dto) {
        return success(dataFlowService.save(dto, getLoginUser()));
    }

    /**
     * 已废弃
     * @return
     */
    @Operation(summary = "Chart")
    @GetMapping("/chart")
    @Deprecated
    public ResponseMessage<Map<String, Object>> chart() {
//        return success(dataflowChartService.chart(getLoginUser()));
        return success(taskService.chart(getLoginUser()));
    }

    @Operation(summary = "Batch start dataFlow")
    @PostMapping("/startBatch")
    public Object startBatch(@RequestBody StateMachineDto dto) {
        if (dto == null || CollectionUtils.isEmpty(dto.getIds())) {
            return failed("InvalidParameter", "ids is empty");
        }
        UserDetail userDetail = getLoginUser();
        return success(dto.getIds().stream().map(id -> getExecResult(id, DataFlowEvent.START, userDetail)).collect(Collectors.toList()));
    }

    @Operation(summary = "Batch stop dataFlow")
    @PostMapping("/stopBatch")
    public Object stopBatch(@RequestBody StateMachineDto dto) {
        if (dto == null || CollectionUtils.isEmpty(dto.getIds())) {
            return failed("InvalidParameter", "ids is empty");
        }
        UserDetail userDetail = getLoginUser();
        return success(dto.getIds().stream().map(id -> getExecResult(id, DataFlowEvent.STOP, userDetail)).collect(Collectors.toList()));
    }

    private StateMachineResult getExecResult(String id, DataFlowEvent event, UserDetail userDetail) {
        StateMachineResult result;
        try {
            DataFlowDto dto = dataFlowService.findById(toObjectId(id), userDetail);
            if (dto == null) {
                throw new BizException("DataFlow.Not.Found");
            }
            result = stateMachineService.executeAboutDataFlow(dto, event, userDetail);
        } catch (Exception e) {
            result = StateMachineResult.fail(e.getMessage());
        }
        return result;
    }

    @Operation(summary = "Find a dataFlow by {{id}} from the data source")
    @GetMapping("/{id}")
    public ResponseMessage<DataFlowDto> findById(@PathVariable("id") String id) {
        return success(dataFlowService.findById(new ObjectId(id), getLoginUser()));
    }

    @Operation(summary = "Patch attributes for a dataFlow and persist it into the data source")
    @PatchMapping("/{id}")
    public ResponseMessage<Long> updateById(@PathVariable("id") String id, @RequestBody DataFlowDto dataFlowDto) {
        return success(dataFlowService.updateById(id, dataFlowDto, getLoginUser()));
    }

    @Operation(summary = "Delete a dataFlow by {{id}} from the data source")
    @DeleteMapping("/{id}")
    public ResponseMessage<Boolean> deleteById(@PathVariable("id") String id) {
        return success(dataFlowService.deleteById(new ObjectId(id), getLoginUser()));
    }

    @Operation(summary = "Copy dataFlow")
    @PostMapping("/{id}/copy")
    public ResponseMessage<DataFlowDto> copyDataFlow(@PathVariable("id") String id) {
        return success(dataFlowService.copyDataFlow(id, getLoginUser()));
    }

    @Operation(summary = "Reset dataFlow")
    @PostMapping("/resetAll")
    public ResponseMessage<DataFlowResetAllResDto> resetDataFlow(@RequestBody DataFlowResetAllReqDto dataFlowResetAllReqDto) {
        return success(dataFlowService.resetDataFlow(dataFlowResetAllReqDto, getLoginUser()));
    }

    @Operation(summary = "Remove dataFlow")
    @PostMapping("/removeAll")
    public ResponseMessage<DataFlowResetAllResDto> removeDataFlow(@RequestParam("where") String whereJson) {
        return success(dataFlowService.removeDataFlow(whereJson, getLoginUser()));
    }

    @Operation(summary = "Count instances of the model matched by where from the data source")
    @GetMapping("/count")
    public ResponseMessage<Object> count(
            @Parameter(in = ParameterIn.QUERY, description = "Criteria to match model instances")
            @RequestParam(value = "where", required = false) String whereJson) {
        Filter filter = parseFilter(whereJson);
        long count = dataFlowService.count(filter.getWhere(), getLoginUser());
        return success(new HashMap() {{
            put("count", count);
        }});
    }


    /**
     * 任务启动 和停止调用该方法
     *
     * @param
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody String reqBody) {
        log.info("dataflow updateByWhere whereJson:{}, reqBody:{}", whereJson, reqBody);
        if (StringUtils.isEmpty(reqBody)) {
            //return failed("InvalidParameter", "reqBody is empty");
            return success(new HashMap<>());
        }


        UserDetail userDetail = getLoginUser();
        Where where = parseWhere(whereJson);
        Document body = Document.parse(reqBody);
        if (!body.containsKey("$set") && !body.containsKey("$setOnInsert") && !body.containsKey("$unset")) {
            Document _body = new Document();
            _body.put("$set", body);
            body = _body;
        }
        Document document = body.get("$set", Document.class);

        DataFlowDto oldDataFlowDto = null;
        Date date = new Date();
        String dataFlowStatus = null;
        if (document.containsKey("status") && where != null && (where.containsKey("_id") || where.containsKey("id"))) {
            String status = document.getString("status");
            Object id = where.get("_id");
            String idString = id.toString();
            oldDataFlowDto=  dataFlowService.findById(MongoUtils.toObjectId(idString));
            switch (status) {
                case "scheduled":  //  scheduled对应startTime和scheduledTime
                    ((Document) body.get("$set")).put("startTime", date);
                    ((Document) body.get("$set")).put("scheduledTime", date);
                    dataFlowStatus = DataFlowRecordDto.STATUS_RUNNING;
                    oldDataFlowDto.setStartTime(date);
                    oldDataFlowDto.setFinishTime(null);
                    break;
                case "stopping":  // stopping对应stoppingTime
                    ((Document) body.get("$set")).put("stoppingTime", date);
                    break;
                case "force stopping":  //  force stopping对应forceStoppingTime
                    ((Document) body.get("$set")).put("forceStoppingTime", date);
                    break;
                case "running":  //  running对应runningTime
                    ((Document) body.get("$set")).put("runningTime", date);
                    break;
                case "error":  //  error对应errorTime和finishTime
                    ((Document) body.get("$set")).put("errorTime", date);
                    ((Document) body.get("$set")).put("finishTime", date);
                    dataFlowStatus = DataFlowRecordDto.STATUS_ERROR;
                    oldDataFlowDto.setFinishTime(date);
                    behaviorService.trace(idString, userDetail, BehaviorCode.errorDataFlow);
                    break;
	            case "paused":  //   paused对应pausedTime和finishTime
		            ((Document) body.get("$set")).put("pausedTime", date);
		            ((Document) body.get("$set")).put("finishTime", date);
                    behaviorService.trace(idString, userDetail, BehaviorCode.pausedDataFlow);
		            boolean isCompleted = false;
		            oldDataFlowDto.setFinishTime(date);
		            /*if (oldDataFlowDto != null && oldDataFlowDto.getSetting() != null && "initial_sync".equals(oldDataFlowDto.getSetting().get("sync_type")) && oldDataFlowDto.getStats() != null){
			            List<String> sourceStageId = oldDataFlowDto.getStages().stream().filter(stage -> stage.get("outputLanes") != null
                                && stage.get("outputLanes") instanceof List
                                && ((List) stage.get("outputLanes")).size() > 0)
                                .map(stage -> stage.get("id").toString())
                                .collect(Collectors.toList());
			            oldDataFlowDto.setFinishTime(date);
			            Object stagesMetrics = oldDataFlowDto.getStats().get("stagesMetrics");
                        if (stagesMetrics instanceof List && CollectionUtils.isNotEmpty((List) stagesMetrics)){
                            int completeCount = (int) IntStream.range(0, ((List) stagesMetrics).size()).mapToObj((IntFunction<Object>) ((List) stagesMetrics)::get)
                                    .filter(stagesMetric -> stagesMetric instanceof Map
                                    && sourceStageId.contains(((Map) stagesMetric).get("stageId").toString())
                                    && "initialized".equals(((Map) stagesMetric).get("status")))
                                    .count();
                            if (sourceStageId.size() == completeCount){
                                isCompleted = true;
                            }
                        }
                    }*/
                    dataFlowStatus = isCompleted ? DataFlowRecordDto.STATUS_COMPLETED : DataFlowRecordDto.STATUS_PAUSED;

                    break;
                default:
                    break;
            }
        }
        long count = dataFlowService.updateByWhere(where, body, userDetail);

        if (count > 0){
            if (StringUtils.isNotBlank(dataFlowStatus) && oldDataFlowDto != null && !oldDataFlowDto.getStatus().equals(document.getString("status"))){
                dataFlowRecordService.saveRecord(oldDataFlowDto, dataFlowStatus, userDetail);
            }
        }

        // dataflow ping
        if (document.size() == 1 && document.containsKey("pingTime") &&
                where != null && where.size() == 1 && where.containsKey("_id")) {
            long size = 0;

            if (where.get("_id") instanceof Map) {
                Map pingIds = (Map) where.get("_id");
                if (pingIds.containsKey("$in") && pingIds.get("$in") instanceof List) {
                    size = ((List) pingIds.get("$in")).size();
                }
            }

            dataFlowPing.increment(size);
        }

        //更新完任务，addMessage
        try {
            String status = document.getString("status");
            if (StringUtils.isNotEmpty(status) && null !=oldDataFlowDto) {
                String name = oldDataFlowDto.getName();
                log.info("dataflow addMessage ,id :{},name :{},status:{}  ", oldDataFlowDto.getId().toString(), name, status);
                if (!oldDataFlowDto.getStatus().equals("error") && "error".equals(status)) {
                    //原来不是error,被更新成error，在新增一条通知
                    messageService.addMigration(name, oldDataFlowDto.getId().toString(), MsgTypeEnum.STOPPED_BY_ERROR, Level.ERROR, userDetail);
                } else if ("running".equals(status)) {
//                    messageService.addMigration(name, idString, MsgTypeEnum.CONNECTED, Level.INFO, getLoginUser());
                }
            }
        } catch (Exception e) {
            log.error("任务状态添加message 异常", e);
        }


		/*if (reqBody.indexOf("\"$set\"") > 0) {
			UpdateDto<DataFlowDto> updateDto = JsonUtil.parseJsonUseJackson(reqBody, new TypeReference<UpdateDto<DataFlowDto>>(){});
			UserDetail userDetail = getLoginUser();
			if (updateDto != null && updateDto.getSet() != null
					&& "running".equals(updateDto.getSet().getStatus())
					&& where != null && where.containsKey("_id")){
				DataFlowDto dataFlowDto = dataFlowService.findById(new ObjectId(where.get("_id").toString()), userDetail);
				if (dataFlowDto != null && "scheduled".equals(dataFlowDto.getStatus())){
					updateDto.getSet().setStartTime(new Date());
				}
			}
			count = dataFlowService.updateByWhere(where, updateDto, userDetail);
		} else {
			DataFlowDto dataFlowDto = JsonUtil.parseJsonUseJackson(reqBody, DataFlowDto.class);
			count = dataFlowService.updateByWhere(where, dataFlowDto, getLoginUser());
		}*/
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    @PostMapping("/tranModelVersionControl")
    public ResponseMessage<Map<String, Boolean>> tranModelVersionControl(@RequestBody TranModelVersionControlReqDto reqDto) {

        List<Stage> stages = reqDto.getStages();

        Graph<Stage, String> graph = new Graph<>();
        for (Stage stage : stages) {
            graph.setNode(stage.getId(), stage);
            if (stage.getInputLanes() != null && stage.getInputLanes().size() > 0) {
                for (String inputLane : stage.getInputLanes()) {
                    graph.setEdge(inputLane, stage.getId());
                }
            }
            if (stage.getOutputLanes() != null && stage.getOutputLanes().size() > 0) {
                for (String output : stage.getOutputLanes()) {
                    graph.setEdge(stage.getId(), output);
                }
                ;
            }
        }

        //a -> b -> c
        //d -> g

        Stream<Graph<Stage, String>> graphList;
        if (reqDto.getStageId() != null) {
            graphList = graph.components().stream().filter(g -> g.hasNode(reqDto.getStageId()));
        } else {
            graphList = graph.components().stream();
        }

        Map<String, Boolean> result = new HashMap<>();
        graphList.forEach(subGraph -> {
            AtomicBoolean subDagSupportMapping = new AtomicBoolean(true);
            for (String nodeId : subGraph.getNodes()) {
                Stage stage = subGraph.getNode(nodeId);
                if (stage.getDatabaseType() != null) {
                    Map<String, List<TypeMappingsEntity>> mappings = typeMappingService.getTypeMapping(stage.getDatabaseType(), TypeMappingDirection.TO_DATATYPE);

                    if (mappings == null || mappings.size() == 0) {
                        subDagSupportMapping.set(false);
                        break;
                    }
                }
            }
            subGraph.getSinks().forEach(id -> result.put(id, subDagSupportMapping.get()));
        });

        return success(result);
    }

    private boolean supportMapping(Map<String, Stage> stageMap, String nodeId, Map<String, Boolean> result) {
        Stage stage = stageMap.get(nodeId);
        if (result.containsKey(nodeId))
            return result.containsKey(nodeId);

        if (stage.getDatabaseType() == null) {
            result.put(nodeId, false);
            return false;
        }
        Map<String, List<TypeMappingsEntity>> mappings = typeMappingService.getTypeMapping(stage.getDatabaseType(), TypeMappingDirection.TO_DATATYPE);
        if (mappings == null || mappings.size() == 0) {
            result.put(nodeId, false);
            return false;
        }


        for (String inputLane : stage.getInputLanes()) {
            boolean sourceSupport = supportMapping(stageMap, inputLane, result);
            if (!sourceSupport) {
                result.put(inputLane, false);
                result.put(nodeId, false);
                return false;
            }
        }
        result.put(nodeId, true);
        return true;
    }

    /**
     * 模型推演
     */
    @PostMapping("/metadata")
    public ResponseMessage<List<SchemaTransformerResult>> transformSchema(@RequestBody DataFlowDto dataFlowDto) {

        if (dataFlowDto.getStages() == null || dataFlowDto.getStages().size() == 0) {
            return success(Collections.emptyList());
        }

        UserDetail user = getLoginUser();

        return success(dataFlowService.transformSchema(dataFlowDto, user));

    }

    /**
     * 模型推演
     */
    @GetMapping("/cron/isValidExpression")
    public ResponseMessage<Object> checkCronExpression(@RequestParam("cron") String cronStr) {

        return success(new HashMap<String, Boolean>(){{put("isValid", CronExpression.isValidExpression(cronStr));}});

    }
}
