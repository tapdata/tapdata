package com.tapdata.tm.task.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.task.param.SaveShareCacheParam;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.vo.ShareCacheDetailVo;
import com.tapdata.tm.task.vo.ShareCacheVo;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;


/**
 * @Date: 2021/11/03
 * @Description:
 */
@Tag(name = "Task", description = "共享缓存接口")
@RestController
@RequestMapping("/api/shareCache")
public class CacheTaskController extends BaseController {

    @Autowired
    private TaskService taskService;

    /**
     * 创建共享缓存依赖任务
     *
     * @return
     */
    @Operation(summary = "创建共享缓存依赖任务")
    @PostMapping
    public ResponseMessage<TaskDto> save(@RequestBody SaveShareCacheParam saveShareCacheParam,
                                         HttpServletRequest request,
                                         HttpServletResponse response) {
        return success(taskService.createShareCacheTask(saveShareCacheParam, getLoginUser(), request, response));
    }



    /**
     * 查询共享缓存列表
     */
    @Operation(summary = "查询共享缓存列表")
    @GetMapping
    public ResponseMessage<Page<ShareCacheVo>> find(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        Where where = filter.getWhere();

        //过滤掉挖掘任务
        HashMap<String, String> logCollectorFilter = new HashMap<>();
        logCollectorFilter.put("$ne", "logCollector");
        where.put("syncType", logCollectorFilter);

        Map notDeleteMap = new HashMap();
        notDeleteMap.put("$ne", true);
        where.put("is_deleted", notDeleteMap);


        where.put("shareCache", true);
        if (where.get("status") == null) {
            Document statusCondition = new Document();
            statusCondition.put("$nin", Lists.of(TaskDto.STATUS_DELETE_FAILED, TaskDto.STATUS_DELETING));
            where.put("status", statusCondition);
        }
        Filter taskFilter = filter;
        UserDetail userDetail = getLoginUser();
        return success(DataPermissionMenuEnums.MemCacheTack.checkAndSetFilter(
                userDetail,
                DataPermissionActionEnums.View,
                () -> taskService.findShareCache(taskFilter, userDetail)
        ));
    }

    /**
     * Patch attributes for a model instance and persist it into the data source
     *
     * @param task
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<TaskDto> updateById(@PathVariable("id") String id, @RequestBody SaveShareCacheParam saveShareCacheParam) {
        UserDetail userDetail = getLoginUser();
        return success(DataPermissionHelper.check(
                userDetail,
                DataPermissionMenuEnums.MemCacheTack,
                DataPermissionActionEnums.Edit,
                DataPermissionDataTypeEnums.Task,
                id,
                () -> taskService.updateShareCacheTask(id, saveShareCacheParam, userDetail),
                () -> {
                    throw new BizException("insufficient.permissions", "", "task.edit");
                }
        ));
    }


    /**
     * 获取共享缓存详情
     *
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "获取共享缓存详情")
    @GetMapping("{id}")
    public ResponseMessage<ShareCacheDetailVo> findById(@PathVariable("id") String id,
                                                        @RequestParam(value = "fields", required = false) String fieldsJson) {
        UserDetail userDetail = getLoginUser();
        return success(checkDataPermission(
                userDetail,
                id,
                DataPermissionActionEnums.View,
                () -> taskService.findShareCacheById(id)
        ));
    }


    /**
     * Delete a model instance by {{id}} from the data source
     *
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        UserDetail userDetail = getLoginUser();
        checkDataPermission(userDetail, id, DataPermissionActionEnums.Delete, () -> {
            taskService.remove(MongoUtils.toObjectId(id), userDetail);
            return null;
        });
        return success();
    }

    /**
     * Find first instance of the model matched by filter from the data source.
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<TaskDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        Where where = filter.getWhere();
        Boolean deleted = (Boolean) where.get("is_deleted");
        if (deleted == null) {
            where.put("is_deleted", false);
        }
        Filter taskFilter = filter;
        UserDetail userDetail = getLoginUser();
        return success(DataPermissionMenuEnums.MemCacheTack.checkAndSetFilter(
                userDetail,
                DataPermissionActionEnums.View,
                () -> taskService.findOne(taskFilter, userDetail)
        ));
    }

    /**
     * Update instances of the model matched by {{where}} from the data source.
     *
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody TaskDto task) {
        Where where = parseWhere(whereJson);
        long count = taskService.updateByWhere(where, task, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    /**
     * Update an existing model instance or insert a new one into the data source based on the where criteria.
     *
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
    @PostMapping("upsertWithWhere")
    public ResponseMessage<TaskDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody TaskDto task) {
        Where where = parseWhere(whereJson);
        return success(taskService.upsertByWhere(where, task, getLoginUser()));
    }


    @Operation(summary = "复制同步任务")
    @PutMapping("copy/{id}")
    public ResponseMessage<TaskDto> copy(@PathVariable("id") String id) {
        return success(taskService.copy(MongoUtils.toObjectId(id), getLoginUser()));
    }


    @Operation(summary = "启动同步任务")
    @PutMapping("start/{id}")
    public ResponseMessage<Void> start(@PathVariable("id") String id) {
        UserDetail userDetail = getLoginUser();
        ObjectId objectId = MongoUtils.toObjectId(id);
        taskService.clearAgentAffinityForManualStart(objectId, userDetail);
        taskService.start(objectId, userDetail);
        return success();
    }

    @Operation(summary = "暂停同步任务")
    //@PutMapping("pause/{id}")
    public ResponseMessage<Void> pause(@PathVariable("id") String id
            , @RequestParam(value = "force", defaultValue = "false") Boolean force) {
        taskService.pause(MongoUtils.toObjectId(id), getLoginUser(), force);
        return success();
    }

    @Operation(summary = "重置同步任务")
    @PutMapping("renew/{id}")
    public ResponseMessage<Void> renew(@PathVariable("id") String id) {
        UserDetail userDetail = getLoginUser();
        checkDataPermission(userDetail, id, DataPermissionActionEnums.Reset, () -> {
            taskService.renew(MongoUtils.toObjectId(id), userDetail);
            return null;
        });
        return success();
    }

    @Operation(summary = "停止同步任务")
    @PutMapping("stop/{id}")
    public ResponseMessage<TaskDto> stop(@PathVariable("id") String id
            , @RequestParam(value = "force", defaultValue = "false") Boolean force) {
        UserDetail userDetail = getLoginUser();
        checkDataPermission(userDetail, id, DataPermissionActionEnums.Stop, () -> {
            taskService.pause(MongoUtils.toObjectId(id), userDetail, force);
            return null;
        });
        return success();
    }


    @PutMapping("batchStart")
    public ResponseMessage<List<MutiResponseMessage>> batchStart(@RequestParam("taskIds") List<String> taskIds,
                                                                 HttpServletRequest request,
                                                                 HttpServletResponse response) {
        List<ObjectId> taskObjectIds = taskIds.stream().map(MongoUtils::toObjectId).collect(Collectors.toList());
        UserDetail userDetail = getLoginUser();
        taskService.clearAgentAffinityForManualStart(taskObjectIds, userDetail);
        List<MutiResponseMessage> responseMessages = taskService.batchStart(taskObjectIds, userDetail, request, response);
        return success(responseMessages);
    }

    @PutMapping("batchStop")
    public ResponseMessage<List<MutiResponseMessage>> batchStop(@RequestParam("taskIds") List<String> taskIds,
                                                                HttpServletRequest request,
                                                                HttpServletResponse response) {
        List<ObjectId> taskObjectIds = taskIds.stream().map(MongoUtils::toObjectId).collect(Collectors.toList());
        UserDetail userDetail = getLoginUser();
        List<MutiResponseMessage> responseMessages = checkDataPermissions(
                userDetail,
                taskObjectIds,
                DataPermissionActionEnums.Stop,
                ids -> taskService.batchStop(ids, userDetail, request, response)
        );
        return success(responseMessages);
    }

    @DeleteMapping("batchDelete")
    public ResponseMessage<List<MutiResponseMessage>> batchDelete(@RequestParam("taskIds") List<String> taskIds,
                                                                  HttpServletRequest request,
                                                                  HttpServletResponse response) {
        List<ObjectId> taskObjectIds = taskIds.stream().map(MongoUtils::toObjectId).collect(Collectors.toList());
        UserDetail userDetail = getLoginUser();
        List<MutiResponseMessage> responseMessages = checkDataPermissions(
                userDetail,
                taskObjectIds,
                DataPermissionActionEnums.Delete,
                ids -> taskService.batchDelete(ids, userDetail, request, response)
        );
        return success(responseMessages);
    }

    private <T> T checkDataPermission(UserDetail userDetail, String id, DataPermissionActionEnums action, Supplier<T> supplier) {
        return DataPermissionHelper.check(
                userDetail,
                DataPermissionMenuEnums.MemCacheTack,
                action,
                DataPermissionDataTypeEnums.Task,
                id,
                supplier,
                () -> dataPermissionUnAuth(action)
        );
    }

    private List<MutiResponseMessage> checkDataPermissions(
            UserDetail userDetail,
            List<ObjectId> ids,
            DataPermissionActionEnums action,
            Function<List<ObjectId>, List<MutiResponseMessage>> supplier
    ) {
        List<MutiResponseMessage> responseMessages = new java.util.ArrayList<>();
        for (ObjectId id : ids) {
            responseMessages.addAll(checkDataPermission(
                    userDetail,
                    id.toHexString(),
                    action,
                    () -> supplier.apply(java.util.Collections.singletonList(id))
            ));
        }
        return responseMessages;
    }

    private <T> T dataPermissionUnAuth(DataPermissionActionEnums action) {
        throw new BizException("insufficient.permissions", "", String.format("task.%s", action.name().toLowerCase()));
    }


}
