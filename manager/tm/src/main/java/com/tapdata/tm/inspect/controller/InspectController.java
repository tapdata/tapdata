package com.tapdata.tm.inspect.controller;

import cn.hutool.core.collection.CollUtil;
import com.alibaba.fastjson.JSON;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.bean.PlatformInfo;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.bean.Source;
import com.tapdata.tm.inspect.bean.Task;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.inspect.service.InspectTaskService;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.permissions.service.DataPermissionService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.stream.Collectors;


/**
 * @Date: 2021/09/14
 * @Description:
 */
@Tag(name = "Inspect", description = "校验任务相关接口")
@Slf4j
@RestController
@RequestMapping("/api/Inspects")
@Setter(onMethod_ = {@Autowired})
public class InspectController extends BaseController {
    private InspectService inspectService;
    private InspectTaskService inspectTaskService;
    private TaskService taskService;

    private <T> T dataPermissionUnAuth(DataPermissionActionEnums action, List<DataPermissionActionEnums> need) {
        throw new BizException("insufficient.permissions",
                needAction(DataPermissionDataTypeEnums.INSPECT, Lists.newArrayList(action)),
                needAction(DataPermissionDataTypeEnums.INSPECT, need)
                );
    }

    protected String needAction(DataPermissionDataTypeEnums dataTypeEnums, List<DataPermissionActionEnums> need) {
        StringJoiner joiner = new StringJoiner(", ");
        need.forEach(a ->
            joiner.add(String.format("%s.%s", dataTypeEnums.getCollection().toLowerCase(), a.name().toLowerCase()))
        );
        return joiner.toString();
    }

    private <T> T dataPermissionCheckOfMenu(UserDetail userDetail,
                                            DataPermissionActionEnums actionEnums,
                                            List<DataPermissionActionEnums> need,
                                            Supplier<T> supplier) {
        return DataPermissionHelper.check(userDetail,
                DataPermissionMenuEnums.INSPECT_TACK,
                actionEnums,
                DataPermissionDataTypeEnums.INSPECT,
                null,
                supplier,
                () -> dataPermissionUnAuth(actionEnums, need));
    }

    private <T> T dataPermissionCheckOfId(HttpServletRequest request,
                                          UserDetail userDetail,
                                          ObjectId id,
                                          DataPermissionActionEnums actionEnums,
                                          List<DataPermissionActionEnums> need,
                                          Supplier<T> supplier) {
        id = Optional.ofNullable(DataPermissionHelper.signDecode(request, id.toHexString())).map(MongoUtils::toObjectId).orElse(id);
        return DataPermissionHelper.checkOfQuery(
                userDetail,
                DataPermissionDataTypeEnums.INSPECT,
                actionEnums,
                inspectService.dataPermissionFindById(id, new Field()),
                (dto) -> DataPermissionMenuEnums.INSPECT_TACK,
                supplier,
                () -> dataPermissionUnAuth(actionEnums, need)
        );
    }

    @GetMapping("/{currentId}/parent-task-sign")
    public ResponseMessage<String> dataPermissionTaskSign(@PathVariable String currentId, @RequestParam String parentId) {
        return success(DataPermissionHelper.signEncode(currentId, parentId));
    }

    /**
     * Create a new instance of the model and persist it into the data source
     *
     * @param inspect
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<InspectDto> save(@RequestBody InspectDto inspect) {
        List task = inspect.getTasks();
        PlatformInfo platformInfo = inspect.getPlatformInfo();
        if (CollectionUtils.isEmpty(task)) {
            throw new BizException("Inspect.task.null");
        }
        inspectService.fieldHandler(task,getLoginUser());
        if (null == platformInfo || StringUtils.isEmpty(platformInfo.getAgentType())) {
            throw new BizException("Inspect.agentTag.null");
        }

        if (inspectService.findByName(inspect.getName()).size() > 0) {
            throw new BizException("Inspect.Name.Exist");
        }

        Date date=new Date();
        inspect.setPing_time(date.getTime());
        return success(inspectService.save(inspect, getLoginUser()));
    }


    /**
     * 编辑inspect属性的时候调用
     *
     * @param inspect
     * @return
     */
    @PatchMapping()
    public ResponseMessage<InspectDto> updateById(HttpServletRequest request, @RequestBody InspectDto inspect) {
        InspectDto resultTask = dataPermissionCheckOfId(request,
                getLoginUser(),
                inspect.getId(),
                DataPermissionActionEnums.Edit,
                Lists.newArrayList(DataPermissionActionEnums.Edit),
                () -> inspectService.updateById(inspect.getId(), inspect, getLoginUser())
        );
        return success(resultTask);
    }


    /**
     * @param filterJson Get inspect task list
     * @return
     */
    @GetMapping
    public ResponseMessage<Page<InspectDto>> find(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        Filter finalFilter = filter;
        Supplier<Page<InspectDto>> supplier = () -> inspectService.list(finalFilter, getLoginUser());
        Page<InspectDto> result = Optional.ofNullable(
                DataPermissionHelper.check(getLoginUser(),
                        DataPermissionMenuEnums.INSPECT_TACK,
                        DataPermissionActionEnums.View,
                        DataPermissionDataTypeEnums.INSPECT,
                        null,
                        supplier,
                        () -> dataPermissionUnAuth(DataPermissionActionEnums.View, Lists.newArrayList(DataPermissionActionEnums.View)))
        )
                .orElseGet(() -> inspectService.list(finalFilter, getLoginUser()));
        return success(result);
    }

    /**
     * Get inspect task details
     *
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("find/{id}")
    public ResponseMessage<InspectDto> findOneById(HttpServletRequest request, @PathVariable("id") String id) {
        InspectDto inspectDto = dataPermissionCheckOfId(
                request,
                getLoginUser(),
                MongoUtils.toObjectId(id),
                DataPermissionActionEnums.View,
                Lists.newArrayList(DataPermissionActionEnums.View),
                () -> inspectService.findOne(Query.query(Criteria.where("_id").is(MongoUtils.toObjectId(id))))
        );
        inspectDto.setTaskDto(findTaskDto(inspectDto.getFlowId()));
        return success(inspectDto);
    }

    /**
     * 获取校验详情
     *
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("findById")
    public ResponseMessage<InspectDto> findById(@RequestParam("filter") String filterStr) {
        Filter filter = parseFilter(filterStr);
        InspectDto inspectDto = dataPermissionCheckOfMenu(
                getLoginUser(),
                DataPermissionActionEnums.View,
                Lists.newArrayList(DataPermissionActionEnums.View),
                () -> inspectService.findById(filter, getLoginUser())
        );
        inspectDto.setTaskDto(findTaskDto(inspectDto.getFlowId()));
        return success(inspectDto);
    }


    /**
     * Delete a model instance by {{id}} from the data source
     *
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Map<String, Long>> delete(HttpServletRequest request, @PathVariable("id") String id) {
        Map<String, Long> deleteResult = dataPermissionCheckOfId(
                request,
                getLoginUser(),
                MongoUtils.toObjectId(id),
                DataPermissionActionEnums.Delete,
                Lists.newArrayList(DataPermissionActionEnums.Delete),
                () -> inspectService.delete(id, getLoginUser())
        );
        return success(deleteResult);
    }


    /**
     * Find first instance of the model matched by filter from the data source.
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<InspectDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        Filter finalFilter = filter;
        InspectDto inspectDto = dataPermissionCheckOfMenu(
                getLoginUser(),
                DataPermissionActionEnums.View,
                Lists.newArrayList(DataPermissionActionEnums.View),
                () -> inspectService.findOne(finalFilter, getLoginUser())
        );
        inspectDto.setTaskDto(findTaskDto(inspectDto.getFlowId()));
        return success(inspectDto);
    }

    protected TaskDto findTaskDto(String taskId) {
        ObjectId id = MongoUtils.toObjectId(taskId);
        TaskDto dto = null;
        if (null != id) {
            dto = taskService.findOne(Query.query(Criteria.where("_id").is(id)));
        }
        return Optional.ofNullable(dto).orElse(new TaskDto());
    }


    /**
     * 页面点击 更新inspect的时候，会调用这个方法
     *
     * @param whereJson
     * @param inspect
     * @return
     */
    @Operation(summary = "页面点击 更新inspect的时候，会调用这个方法")
    @PostMapping("execute")
    public ResponseMessage<InspectDto> updateByWhere(HttpServletRequest request, @RequestParam("where") String whereJson, @RequestBody InspectDto inspect) {
        log.info("Inspect Controller -- do execute -  where json：{}, inspect：{} ", whereJson, JSON.toJSONString(inspect));
        whereJson = whereJson.replace("\"_id\"", "\"id\"");
        Where where = parseWhere(whereJson);
        ObjectId id = Optional.ofNullable(inspect.getId()).orElse(MongoUtils.toObjectId(String.valueOf(where.get("id"))));
        String status = inspect.getStatus();
        DataPermissionActionEnums verifyAction = null;
        List<DataPermissionActionEnums> needActionEnums = Lists.newArrayList(DataPermissionActionEnums.Edit);
        //按照操作类型来确认需要校验的权限
        if (InspectStatusEnum.SCHEDULING.getValue().equals(status)) {
            verifyAction = DataPermissionActionEnums.Start;
            needActionEnums.add(verifyAction);
        } else if (InspectStatusEnum.STOPPING.getValue().equals(status)) {
            verifyAction = DataPermissionActionEnums.Stop;
            needActionEnums.add(verifyAction);
        }
        //检查是否有编辑权限
        dataPermissionCheckOfId(
                request,
                getLoginUser(),
                id,
                DataPermissionActionEnums.Edit,
                needActionEnums,
                () -> inspect);

        InspectDto inspectDto;
        if (null != verifyAction) {
            inspectDto = dataPermissionCheckOfId(
                    request,
                    getLoginUser(),
                    id,
                    verifyAction,
                    needActionEnums,
                    () -> inspectService.doExecuteInspect(where, inspect, getLoginUser())
            );
        } else {
            inspectDto = inspectService.doExecuteInspect(where, inspect, getLoginUser());
        }
        return success(inspectDto);
    }


    @Operation(summary = "engine 更新inspect的时候，都会调用这个方法")
    @PostMapping("update")
    public ResponseMessage<InspectDto> updateByWhere(@RequestParam("where") String whereJson, @RequestBody InspectDto inspect) {
        log.info("engine 调用了 updateByWhere.  where :{}  InspectDto:{} ", whereJson, JsonUtil.toJson(inspect));
        whereJson = whereJson.replace("\"_id\"", "\"id\"");
        Where where = parseWhere(whereJson);
        return success(inspectService.doExecuteInspect(where, inspect, getLoginUser()));
    }

    @Operation(summary = "详情页查询任务列表的时候，都会调用这个方法")
    @GetMapping("task-list")
    public ResponseMessage<List<TaskDto>> getTaskDtoList() {
        return success(inspectTaskService.findTaskList(getLoginUser()));
    }

    /**
     * 给engin调用，更新inspect状态的
     * engin  调用该方法，result  总是返回空ss字符串，导致inspect表更新总是不正确，因此需要另作调整
     *
     * @param whereJson
     * @param inspect
     * @return
     */
    @Operation(summary = "给engin调用，更新inspect状态的")
    @PostMapping("upsertWithWhere")
    public ResponseMessage<InspectDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody InspectDto inspect) {
        log.info("engin upsertWithWhere whereJson:{}, inspect:{} ", whereJson, JSON.toJSONString(inspect));
        Where where = parseWhere(whereJson);
        Long inspectDto = inspectService.updateByWhere(where, inspect, getLoginUser());
        return success(inspect);
    }

}
