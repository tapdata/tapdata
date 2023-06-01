package com.tapdata.tm.ds.controller;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.CreateTypeEnum;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.bean.NoSchemaFilter;
import com.tapdata.tm.ds.dto.ConnectionStats;
import com.tapdata.tm.ds.dto.UpdateTagsDto;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.param.ValidateTableParam;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.ds.vo.AllDataSourceConnectionVo;
import com.tapdata.tm.ds.vo.ValidateTableVo;
import com.tapdata.tm.metadatadefinition.param.BatchUpdateParam;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.BeanUtil;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeHolder;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import lombok.Setter;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.*;
import java.util.stream.Collectors;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

/**
 * @Author: Zed
 * @Date: 2021/8/19
 * @Description: 数据源连接相关接口
 */
@RestController
@CrossOrigin("*")
@Tag(name = "Connections", description = "数据源连接相关接口")
@RequestMapping({"api/Connections", "/api/v2/ds"})
@Setter(onMethod_ = {@Autowired})
public class DataSourceController extends BaseController {

    private DataSourceService dataSourceService;
    private TaskService taskService;

    private MetadataDefinitionService metadataDefinitionService;

    /**
     * 添加数据源连接
     *
     * @param connection 数据源连接实体
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "添加数据源连接")
    @PostMapping
    public ResponseMessage<DataSourceConnectionDto> add(@RequestBody DataSourceConnectionDto connection, @RequestParam(name = "id", required = false) String id) {
        if (StringUtils.isNotBlank(id)) {
            connection.setId(new ObjectId(id));
            return success(dataSourceService.addWithSpecifiedId(connection, getLoginUser()));
        } else {
            connection.setId(null);
            return success(dataSourceService.add(connection, getLoginUser()));
        }
    }

    /**
     * 修改数据源
     *
     * @param updateDto 修改后的名称
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "修改数据源连接")
    @PatchMapping
    public ResponseMessage<DataSourceConnectionDto> update(@RequestBody DataSourceConnectionDto updateDto) {
        return success(dataSourceService.update(getLoginUser(), updateDto, !(
                DataSourceEntity.STATUS_TESTING.equals(updateDto.getStatus()) || "loading".equals(updateDto.getLoadFieldsStatus()) // 界面连接测试和加载模型不修改时间
        )));
    }

    /**
     * 测试连接时，以及修改属性，都调用该方法
     *
     * @param dataSource dataSource
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<DataSourceConnectionDto> updateById(@PathVariable("id") String id, @RequestBody(required = false) DataSourceConnectionDto dataSource) {
        UserDetail userDetail = getLoginUser();
        if (dataSource == null) {
            dataSource = new DataSourceConnectionDto();
        } else if (Boolean.FALSE.equals(dataSource.getHeartbeatEnable())) {
            taskService.deleteHeartbeatByConnId(userDetail, id);
        }
        dataSource.setId(MongoUtils.toObjectId(id));
        return success(dataSourceService.update(userDetail, dataSource, !(
                DataSourceEntity.STATUS_TESTING.equals(dataSource.getStatus()) || "loading".equals(dataSource.getLoadFieldsStatus()) // 界面连接测试和加载模型不修改时间
        )));
    }

    /**
     * 根据条件查询数据源连接列表
     *
     * @param filterJson filterJson
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "根据条件查询数据源连接列表")
    @GetMapping
    public ResponseMessage<Page<DataSourceConnectionDto>> find(@RequestParam(value = "filter", required = false) String filterJson,
                                                               @RequestParam(value = "noSchema", required = false) Boolean noSchema) {
        filterJson = replaceLoopBack(filterJson);
        NoSchemaFilter filter = JsonUtil.parseJson(filterJson, NoSchemaFilter.class);
        if (filter == null) {
            filter = new NoSchemaFilter();
        }

        if (noSchema == null) {
            noSchema = true;
            if (filter.getNoSchema() != null) {
                noSchema = filter.getNoSchema() == 1;
            } else if (filter.getFields() != null && filter.getFields().size() != 0) {
                Object schema = filter.getFields().get("schema");
                noSchema = (schema == null);
            }
        }


        //隐藏密码
        Page<DataSourceConnectionDto> dataSourceConnectionDtoPage = dataSourceService.list(filter, noSchema, getLoginUser());

        return success(dataSourceConnectionDtoPage);
    }


    /**
     * 根据条件查询数据源连接列表
     *
     * @param filterJson filterJson
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "根据条件查询数据源连接列表,不分页")
    @GetMapping("findAll")
    public ResponseMessage<List<AllDataSourceConnectionVo>> findAll(@RequestParam(value = "filter", required = false) String filterJson) {
        filterJson = replaceLoopBack(filterJson);
        NoSchemaFilter filter = JsonUtil.parseJson(filterJson, NoSchemaFilter.class);
        if (filter == null) {
            filter = new NoSchemaFilter();
        }
        Where where = filter.getWhere();
        if (where == null) {
            where = new Where();
            Map<String, Object> createTypeMap = new HashMap<>();
            createTypeMap.put("$ne", CreateTypeEnum.System);
            where.put("createType", createTypeMap);
        }

        //隐藏密码
        List<DataSourceConnectionDto> dataSourceConnectionDtoList = dataSourceService.findAll(filter.getWhere(), getLoginUser());
        List<AllDataSourceConnectionVo> allDataSourceConnectionVoList = BeanUtil.deepCloneList(dataSourceConnectionDtoList, AllDataSourceConnectionVo.class);

        return success(allDataSourceConnectionVoList);
    }


    /**
     * 根据条件查询数据源连接列表
     *
     * @param filterJson filterJson
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "根据条件查询数据源连接列表,不分页")
    @GetMapping("listAll")
    public ResponseMessage<List<DataSourceConnectionDto>> logCollectorInfo(@RequestParam(value = "filter", required = false) String filterJson) {
        filterJson = replaceLoopBack(filterJson);
        NoSchemaFilter filter = JsonUtil.parseJson(filterJson, NoSchemaFilter.class);
        if (filter == null) {
            filter = new NoSchemaFilter();
        }

        Where where = filter.getWhere();
        if (where == null) {
            where = new Where();
            Map<String, Object> createTypeMap = new HashMap<>();
            createTypeMap.put("$ne", CreateTypeEnum.System);
            where.put("createType", createTypeMap);
        }

        return success(dataSourceService.listAll(filter, getLoginUser()));
    }

    /**
     * Replace an existing model instance or insert a new one into the data source
     *
     * @param dataSource dataSource
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<DataSourceConnectionDto> put(@RequestBody DataSourceConnectionDto dataSource) {
        return success(dataSourceService.replaceOrInsert(dataSource, getLoginUser()));
    }


    /**
     * Check whether a model instance exists in the data source
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        long count = dataSourceService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     * 给数据源连接修改资源分类
     *
     * @param updateTagsDto 数据源连接id列表
     * @return DataSourceConnectionDto
     */
    @Deprecated
    @Operation(summary = "给数据源连接修改资源分类")
    @PatchMapping("batchUpdateListtag")
    public ResponseMessage<DataSourceConnectionDto> updateTag(@RequestBody UpdateTagsDto updateTagsDto) {
        dataSourceService.updateTag(getLoginUser(), updateTagsDto);
        return success();
    }

    /**
     * 根据id查询数据源连接，需要判断用户id
     *
     * @param id id
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "根据id查询数据源连接")
    @GetMapping("{id}")
    public ResponseMessage<DataSourceConnectionDto> findById(@PathVariable("id") String id, @RequestParam(value = "fields", required = false) String fieldsJson,
                                                             @RequestParam(value = "noSchema", required = false) String noSchema) {
        Field fields = parseField(fieldsJson);

        boolean no = true;
        if (noSchema != null) {
            no = ("1".equals(noSchema) || "true".equals(noSchema));
        }
        return success(dataSourceService.getById(toObjectId(id), fields, no, getLoginUser()));
    }

    /**
     * Replace attributes for a model instance and persist it into the data source.
     *
     * @param dataSource dataSource
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PutMapping("{id}")
    public ResponseMessage<DataSourceConnectionDto> replaceById(@PathVariable("id") String id, @RequestBody DataSourceConnectionDto dataSource) {
        UserDetail user = getLoginUser();
        Boolean submit = dataSource.getSubmit();
        dataSourceService.updateCheck(user, dataSource);
        DataSourceConnectionDto connectionDto = dataSourceService.replaceById(toObjectId(id), dataSource, getLoginUser());
        dataSourceService.updateAfter(user, connectionDto, submit);
        return success(connectionDto);
    }


    /**
     * Replace attributes for a model instance and persist it into the data source.
     *
     * @param dataSource dataSource
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PostMapping("{id}/replace")
    public ResponseMessage<DataSourceConnectionDto> replaceById2(@PathVariable("id") String id, @RequestBody DataSourceConnectionDto dataSource) {
        UserDetail user = getLoginUser();
        Boolean submit = dataSource.getSubmit();
        dataSourceService.updateCheck(user, dataSource);
        DataSourceConnectionDto connectionDto = dataSourceService.replaceById(toObjectId(id), dataSource, user);
        dataSourceService.updateAfter(user, connectionDto, submit);
        return success(connectionDto);
    }


    /**
     * 删除数据源连接
     *
     * @param id id
     */
    @Operation(summary = "删除数据源连接")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        dataSourceService.delete(getLoginUser(), id);
        return success();
    }

    /**
     * Check whether a model instance exists in the data source
     *
     * @param id id
     * @return map
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @GetMapping("{id}/exists")
    public ResponseMessage<HashMap<String, Boolean>> checkById1(@PathVariable("id") String id) {
        long count = dataSourceService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }


    /**
     * 根据id查询数据源连接，需要判断用户id
     *
     * @param id id
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "根据id查询数据源连接")
    @GetMapping("{id}/customQuery")
    public ResponseMessage<DataSourceConnectionDto> customQuery(@PathVariable("id") String id,
                                                                @RequestParam(value = "tableName", required = false) String tableName,
                                                                @RequestParam(value = "schema", required = false) Boolean schema) {
        return success(dataSourceService.customQuery(toObjectId(id), tableName, schema, getLoginUser()));
    }


    /**
     * 根据条件查询数据源连接列表
     *
     * @param whereJson whereJson
     * @return map
     */
    @Operation(summary = "根据条件查询数据源连接列表")
    @GetMapping("count")
    public ResponseMessage<Map<String, Long>> count(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "where", required = false) String whereJson) {
        Where where = parseWhere(whereJson);

        Map<String, Object> createTypeMap = new HashMap<>();
        createTypeMap.put("$ne", CreateTypeEnum.System);
        where.put("createType", createTypeMap);

        long count = dataSourceService.count(where, getLoginUser());


        HashMap<String, Long> returnMap = new HashMap<>();
        returnMap.put("count", count);

        return success(returnMap);
    }


    /**
     * Find first instance of the model matched by filter from the data source.
     *
     * @param filterJson filterJson
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<DataSourceConnectionDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        filterJson = replaceLoopBack(filterJson);
        NoSchemaFilter filter = JsonUtil.parseJson(filterJson, NoSchemaFilter.class);
        if (filter == null) {
            filter = new NoSchemaFilter();
        }


        boolean noSchema = true;
        if (filter.getNoSchema() != null) {
            noSchema = filter.getNoSchema() == 1;
        } else if (filter.getFields() != null && filter.getFields().size() != 0) {
            Object schema = filter.getFields().get("schema");
            noSchema = (schema == null);
        }

        return success(dataSourceService.findOne(filter, getLoginUser(), noSchema));
    }

    /**
     * 复制数据源连接
     *
     * @param id id
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "复制数据源")
    @PostMapping("{id}/copy")
    public ResponseMessage<DataSourceConnectionDto> copy(@PathVariable("id") String id, HttpServletRequest request) {
        return success(dataSourceService.copy(getLoginUser(), id, request.getRequestURI()));
    }

    /**
     * Update instances of the model matched by {{where}} from the data source.
     *
     * @param whereJson whereJson
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody String reqBody) {
        Where where = parseWhere(whereJson);

        long count;
        UserDetail user = getLoginUser();
        if (reqBody.indexOf("\"$set\"") > 0 || reqBody.indexOf("\"$setOnInsert\"") > 0 || reqBody.indexOf("\"$unset\"") > 0) {
            Document updateDto = InstanceFactory.instance(JsonParser.class).fromJson(reqBody, Document.class);
            count = dataSourceService.upsertByWhere(where, updateDto, null, user);
        } else {
            DataSourceConnectionDto connectionDto = JsonUtil.parseJsonUseJackson(reqBody, DataSourceConnectionDto.class);
            count = dataSourceService.upsertByWhere(where, null, connectionDto, user);
        }
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    /**
     * Update instances of the model matched by {{where}} from the data source.
     *
     * @param whereJson whereJson
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("not-change-last/update")
    public ResponseMessage<Map<String, Long>> updateByWhereNotChangeLast(@RequestParam("where") String whereJson, @RequestBody String reqBody) {
        Where where = parseWhere(whereJson);

        long count;
        UserDetail user = getLoginUser();
        if (reqBody.indexOf("\"$set\"") > 0 || reqBody.indexOf("\"$setOnInsert\"") > 0 || reqBody.indexOf("\"$unset\"") > 0) {
            Document updateDto = InstanceFactory.instance(JsonParser.class).fromJson(reqBody, Document.class);
            count = dataSourceService.upsertByWhere(where, updateDto, null, user);
        } else {
            DataSourceConnectionDto connectionDto = JsonUtil.parseJsonUseJackson(reqBody, DataSourceConnectionDto.class);
            count = dataSourceService.upsertByWhere(where, null, connectionDto, user);
        }
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }


    /**
     * Update an existing model instance or insert a new one into the data source based on the where criteria.
     *
     * @param whereJson whereJson
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
    @PostMapping("upsertWithWhere")
    public ResponseMessage<DataSourceConnectionDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody DataSourceConnectionDto dataSource) {
        Where where = parseWhere(whereJson);
        return success(dataSourceService.upsertByWhere(where, dataSource, getLoginUser()));
    }


    @GetMapping("distinct/{field}")
    public ResponseMessage<List<String>> distinct(@PathVariable String field) {
        return success(dataSourceService.distinct(field, getLoginUser()));
    }

    @GetMapping("databaseType")
    public ResponseMessage<List<String>> databaseType() {
        return success(dataSourceService.databaseType(getLoginUser()));
    }

    /**
     * 校验表是否属于某个数据库
     */
    @Operation(summary = " 校验表是否属于某个数据库")
    @PostMapping("validateTable")
    public ResponseMessage<ValidateTableVo> validateTable(@RequestBody ValidateTableParam validateTableParam) {
        return success(dataSourceService.validateTable(validateTableParam.getConnectionId(), validateTableParam.getTableList()));
    }

    /**
     *
     */
    @Operation(summary = " 批量修改所属类别")
    @PatchMapping("batchUpdateListtags")
    public ResponseMessage<List<String>> batchUpdateListTags(@RequestBody BatchUpdateParam batchUpdateParam) {
        return success(metadataDefinitionService.batchUpdateListTags("Connections", batchUpdateParam, getLoginUser()));
    }

    /**
     * 现在除了 kafka, elasticsearch以外  都支持数据校验
     */
    @Operation(summary = "数据源支持功能列表")
    @GetMapping("supportList")
    public ResponseMessage<List> supportList() {
        return success(dataSourceService.supportList(getLoginUser()));
    }


    @PostMapping("connectionOptions/update")
    public ResponseMessage<Void> updateConnectionOptions(@RequestParam("where") String whereJson, @RequestBody String reqBody) {
        Where where = parseWhere(whereJson);
        if (reqBody.indexOf("\"$set\"") > 0 || reqBody.indexOf("\"$setOnInsert\"") > 0 || reqBody.indexOf("\"$unset\"") > 0) {
            Document updateDto = InstanceFactory.instance(JsonParser.class).fromJson(reqBody, Document.class);
            JSONObject set = (JSONObject) updateDto.get("$set");
            ConnectionOptions options = null;
            if (set.get("options") != null) {
                options = JsonUtil.parseJsonUseJackson(JsonUtil.toJson(set.get("options")), new TypeReference<ConnectionOptions>() {
                });
            }
            dataSourceService.updateConnectionOptions(MongoUtils.toObjectId((String) where.get("_id")), options, getLoginUser());
        }
        return success();
    }

    @Operation(summary = "Find tasks referencing the current connection")
    @GetMapping("task/{id}/{limit}")
    public ResponseMessage<Map<String, Object>> findTaskByConnectionId(@PathVariable("id") String connectionId, @PathVariable("limit") int limit) {
        UserDetail loginUser = getLoginUser();
        Long total = dataSourceService.countTaskByConnectionId(connectionId, loginUser);
        Long logTotal = dataSourceService.countTaskByConnectionId(connectionId,"logCollector",loginUser);
        List<TaskDto> taskList = dataSourceService.findTaskByConnectionId(connectionId, limit, loginUser);
        List<TaskDto> logTaskList = dataSourceService.findTaskByConnectionId(connectionId, limit,"logCollector", loginUser);
        List<Document> items = taskList.stream()
                .map(task -> new Document("id", task.getId().toHexString())
                        .append("name", task.getName())
                        .append("syncType", task.getSyncType())).collect(Collectors.toList());
        List<Document> logItems = logTaskList.stream()
                .map(task -> new Document("id", task.getId().toHexString())
                        .append("name", task.getName())
                        .append("syncType", task.getSyncType())).collect(Collectors.toList());
        items.addAll(logItems);
        Map<String, Object> result = new HashMap<String, Object>() {{
            put("items", items);
            put("total", total+logTotal);
        }};
        return success(result);
    }

    @Operation(summary = "Find logCollector tasks referencing the current connection")
    @GetMapping("logCollectorTask/{id}/{limit}")
    public ResponseMessage<Map<String, Object>> findLogCollectorTaskByConnectionId(@PathVariable("id") String connectionId, @PathVariable("limit") int limit) {
        UserDetail loginUser = getLoginUser();
        Long total = dataSourceService.countTaskByConnectionId(connectionId, TaskDto.SYNC_TYPE_LOG_COLLECTOR, loginUser);
        List<TaskDto> taskList = dataSourceService.findTaskByConnectionId(connectionId, limit, TaskDto.SYNC_TYPE_LOG_COLLECTOR, loginUser);
        List<Document> items = taskList.stream()
                .map(task -> new Document("id", task.getId().toHexString())
                        .append("name", task.getName())
                        .append("syncType", task.getSyncType())).collect(Collectors.toList());
        Map<String, Object> result = new HashMap<String, Object>() {{
            put("items", items);
            put("total", total);
        }};
        return success(result);
    }

    @Operation(summary = "Connection statistics api")
    @GetMapping("/stats")
    public ResponseMessage<ConnectionStats> stats() {

        return success(dataSourceService.stats(getLoginUser()));

    }

    @Operation(summary = "加载部分表")
    @PostMapping("load/part/tables/{connectionId}")
    public ResponseMessage<Void> loadPartTables(@PathVariable("connectionId") String connectionId, @RequestBody String param) {
        List<TapTable> tables = InstanceFactory.instance(JsonParser.class).fromJson(param, new TypeHolder<List<TapTable>>() {});
        dataSourceService.loadPartTables(connectionId, tables, getLoginUser());
        return success();

    }

    @Operation(summary = "Get connection heartbeat task")
    @GetMapping("/{id}/heartbeat-task")
    public ResponseMessage<Set<String>> getConnectionHeartbeatTask(@PathVariable("id") String connectionId) {
        Set<String> taskIds = new HashSet<>();
        List<TaskDto> tasks = taskService.findHeartbeatByConnectionId(connectionId, "_id");
        if (null != tasks) {
            for (TaskDto dto : tasks) {
                taskIds.add(dto.getId().toHexString());
            }
        }
        return success(taskIds);
    }

    @Operation(summary = "根据连接ID查找数据源正在使用外存的任务")
    @GetMapping("{id}/usingTaskByConnectionId")
    public ResponseMessage<Map<String, Object>>getUsingTaskByConnectionId(@PathVariable("id") String id) {
        UserDetail userDetail = getLoginUser();
        List<TaskDto> tasks = dataSourceService.findUsingTaskByConnectionId(id,userDetail);
        Long total = dataSourceService.countUsingTaskByConnectionId(id,userDetail);
        Map<String, Object> result = new HashMap<String, Object>() {{
            put("items", tasks);
            put("total", total);
        }};
        return success(result);
    }

}
