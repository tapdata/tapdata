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
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.inspect.bean.Source;
import com.tapdata.tm.inspect.bean.Task;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectResultService;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.inspect.service.InspectTaskService;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.task.service.TaskService;
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
    private InspectResultService inspectResultService;
    private TaskService taskService;
    private DataSourceService dataSourceService;

    private <T> T dataPermissionUnAuth() {
        throw new RuntimeException("Un auth");
    }

    private <T> T dataPermissionCheckOfMenu(UserDetail userDetail, DataPermissionActionEnums actionEnums, Supplier<T> supplier) {
        return DataPermissionHelper.check(userDetail,
                DataPermissionMenuEnums.InspectTack,
                actionEnums,
                DataPermissionDataTypeEnums.Inspect,
                null,
                supplier,
                this::dataPermissionUnAuth);
    }

    private <T> T dataPermissionCheckOfId(HttpServletRequest request,
                                          UserDetail userDetail,
                                          ObjectId id,
                                          DataPermissionActionEnums actionEnums,
                                          Supplier<T> supplier) {
        id = Optional.ofNullable(DataPermissionHelper.signDecode(request, id.toHexString())).map(MongoUtils::toObjectId).orElse(id);
        return DataPermissionHelper.checkOfQuery(
                userDetail,
                DataPermissionDataTypeEnums.Inspect,
                actionEnums,
                inspectService.dataPermissionFindById(id, new Field()),
                (dto) -> DataPermissionMenuEnums.InspectTack,
                supplier,
                this::dataPermissionUnAuth
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
                () -> inspectService.updateById(inspect.getId(), inspect, getLoginUser())
        );
        return success(inspect);
    }


    /**
     * @param filterJson 获取校验任务列表
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
                        DataPermissionMenuEnums.InspectTack,
                        DataPermissionActionEnums.View,
                        DataPermissionDataTypeEnums.Inspect,
                        null,
                        supplier,
                        this::dataPermissionUnAuth)
        )
                .orElseGet(() -> inspectService.list(finalFilter, getLoginUser()));
        return success(result);
    }

    /**
     * 获取校验详情
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
                () -> inspectService.findOne(Query.query(Criteria.where("_id").is(MongoUtils.toObjectId(id))))
        );
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
                () -> inspectService.findById(filter, getLoginUser())
        );
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
                () -> inspectService.findOne(finalFilter, getLoginUser())
        );
        return success(inspectDto);
    }


    /**
     * 页面点击 更新inspect的时候，会调用这个方法
     *
     * @param whereJson
     * @param inspect
     * @return
     */
    @Operation(summary = "页面点击 更新inspect的时候，会调用这个方法")
    @PostMapping("update")
    public ResponseMessage<InspectDto> updateByWhere(HttpServletRequest request, @RequestParam("where") String whereJson, @RequestBody InspectDto inspect) throws Exception {
        log.info("InspectController--updateByWhere。whereJson：{}， InspectDto：{} ", whereJson, JSON.toJSONString(inspect));
        whereJson = whereJson.replace("\"_id\"", "\"id\"");
        Where where = parseWhere(whereJson);
        ObjectId id = Optional.ofNullable(inspect.getId()).orElse(MongoUtils.toObjectId(String.valueOf(where.get("id"))));
        String status = inspect.getStatus();
        DataPermissionActionEnums verifyAction = null;
        //检查是否有编辑权限
        dataPermissionCheckOfId(
                request,
                getLoginUser(),
                id,
                DataPermissionActionEnums.Edit, () -> inspect);
        //按照操作类型来确认需要校验的权限
        if (InspectStatusEnum.SCHEDULING.getValue().equals(status)) {
            verifyAction = DataPermissionActionEnums.Start;
        } else if (InspectStatusEnum.STOPPING.getValue().equals(status)) {
            verifyAction = DataPermissionActionEnums.Stop;
        }
        InspectDto inspectDto;
        if (null != verifyAction) {
            inspectDto = dataPermissionCheckOfId(
                    request,
                    getLoginUser(),
                    id,
                    verifyAction,
                    () -> inspectService.doExecuteInspect(where, inspect, getLoginUser())
            );
        } else {
            inspectDto = inspectService.doExecuteInspect(where, inspect, getLoginUser());
        }
        return success(inspectDto);
    }


    @Operation(summary = "engine 更新inspect的时候，都会调用这个方法")
    @PostMapping("do-execute-for-engine")
    public ResponseMessage<InspectDto> updateByWhere(@RequestParam("where") String whereJson, @RequestBody InspectDto inspect) throws Exception {
        log.info("InspectController--updateByWhere。whereJson：{}， InspectDto：{} ", whereJson, JSON.toJSONString(inspect));
        whereJson = whereJson.replace("\"_id\"", "\"id\"");
        Where where = parseWhere(whereJson);
        return success(inspectService.doExecuteInspect(where, inspect, getLoginUser()));
    }

    @Operation(summary = "详情页查询任务列表的时候，都会调用这个方法")
    @GetMapping("task-list/{inspectId}")
    public ResponseMessage<List<TaskDto>> getTaskDtoListInInspectInfoPage(@PathVariable("inspectId") String inspectId) {
        InspectDto inspectDto = getDto(inspectId);
        String taskId = inspectDto.getFlowId();
        Supplier<TaskDto> taskDtoSupplier = taskService.dataPermissionFindById(MongoUtils.toObjectId(taskId), null);
        TaskDto taskDto = taskDtoSupplier.get();
        if (null == taskDto) {
            throw new BizException("Task Not exists");
        }
        List<TaskDto> taskList = inspectTaskService.findTaskList(getLoginUser());
        if (CollUtil.isEmpty(taskList)) {
            throw new BizException("Not Auth to get Tasks");
        }
        Optional<TaskDto> first = taskList.stream().filter(Objects::nonNull).filter(t -> taskId.equals(t.getId().toHexString())).findFirst();
        if (!first.isPresent()) {
            throw new BizException("查看权限不完整，部分任务为开放查询权限，请联系管理员Not Auth to get Tasks");
        }
        return success(taskList);
    }

    protected InspectDto getDto(String inspectId) {
        return dataPermissionCheckOfMenu(
                getLoginUser(),
                DataPermissionActionEnums.View,
                () -> inspectService.findOne(Query.query(Criteria.where("_id").is(MongoUtils.toObjectId(inspectId))), getLoginUser())
        );
    }

    @Operation(summary = "详情页查询数据源列表的时候，都会调用这个方法")
    @GetMapping("connector-list/{inspectId}")
    public ResponseMessage<List<DataSourceConnectionDto>> getConnectorDtoListInInspectInfoPage(@PathVariable("inspectId") String inspectId) {
        InspectDto inspectDto = getDto(inspectId);
        List<DataSourceConnectionDto> connectionList = inspectTaskService.findConnectionList(getLoginUser());
        Map<String, DataSourceConnectionDto> collect = connectionList.stream().filter(Objects::nonNull).collect(Collectors.toMap(c -> c.getId().toHexString(), c -> c, (c1, c2) -> c1));
        //检查能否获取数据源列表
        List<Task> tasks = inspectDto.getTasks();
        for (int index = 0; index < tasks.size(); index++) {
            Task task = tasks.get(index);
            checkConnection(task.getSource(), collect);
            checkConnection(task.getTarget(), collect);
        }
        return success(connectionList);
    }

    protected void checkConnection(Source connection, Map<String, DataSourceConnectionDto> collect) {
        //查看权限不完整，部分数据源连接未开放查询权限，请联系管理员
        String connectionId = connection.getConnectionId();
        DataSourceConnectionDto connectionDto = collect.get(connectionId);
        if (null == connectionDto) {
            throw new BizException("查看权限不完整，部分任务为开放查询权限，请联系管理员Not Auth to get Tasks");
        }
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
