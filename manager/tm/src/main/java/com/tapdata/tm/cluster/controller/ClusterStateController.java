package com.tapdata.tm.cluster.controller;

import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.cluster.dto.AccessNodeInfo;
import com.tapdata.tm.cluster.dto.ClusterStateDto;
import com.tapdata.tm.cluster.dto.ClusterStateMonitorRequest;
import com.tapdata.tm.cluster.dto.OracleLogParserCommandExecResult;
import com.tapdata.tm.cluster.dto.OracleLogParserSNResult;
import com.tapdata.tm.cluster.dto.OracleLogParserUpdateConfigResult;
import com.tapdata.tm.cluster.dto.OracleLogParserUpgradeSNResult;
import com.tapdata.tm.cluster.dto.RawServerStateDto;
import com.tapdata.tm.cluster.dto.UpdataStatusRequest;
import com.tapdata.tm.cluster.dto.UpdateAgentVersionParam;
import com.tapdata.tm.cluster.params.OracleLogParserConfigParam;
import com.tapdata.tm.cluster.service.ClusterStateService;
import com.tapdata.tm.cluster.service.OracleLogParserService;
import com.tapdata.tm.cluster.service.RawServerStateService;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @Date: 2021/09/13
 * @Description:
 */
@Tag(name = "ClusterState", description = "数据源模型相关接口")
@RestController
@RequestMapping("api/clusterStates")
@Setter(onMethod_ = {@Autowired})
public class ClusterStateController extends BaseController {

    private ClusterStateService clusterStateService;
    private RawServerStateService rawServerStateService;

    @Autowired
    private PermissionService permissionService;
    @Autowired
    private SettingsService settingsService;
    private OracleLogParserService oracleLogParserService;

    /**
     * Create a new instance of the model and persist it into the data source
     * @param clusterState clusterState
     * @return ClusterStateDto
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<ClusterStateDto> save(@RequestBody ClusterStateDto clusterState) {
        clusterState.setId(null);
        return success(clusterStateService.save(clusterState, getLoginUser()));
    }

    /**
     *  Patch an existing model instance or insert a new one into the data source
     * @param clusterState clusterState
     * @return ClusterStateDto
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<ClusterStateDto> update(@RequestBody ClusterStateDto clusterState) {
        return success(clusterStateService.save(clusterState, getLoginUser()));
    }


    /**
     * Find all instances of the model matched by filter from the data source
     * @param filterJson filterJson
     * @return ClusterStateDto
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<ClusterStateDto>> find(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(clusterStateService.getAll(filter));
    }

    /**
     *  Replace an existing model instance or insert a new one into the data source
     * @param clusterState clusterState
     * @return ClusterStateDto
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<ClusterStateDto> put(@RequestBody ClusterStateDto clusterState) {
        if (settingsService.isCloud() || Boolean.TRUE.equals(permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_CLUSTER_MANAGEMENT, getLoginUser().getUserId()))) {
            return success(clusterStateService.replaceOrInsert(clusterState, getLoginUser()));
        }else{
            throw new BizException("NotAuthorized");
        }

    }


    /**
     * Check whether a model instance exists in the data source
     * @return map
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        long count = clusterStateService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     *  Patch attributes for a model instance and persist it into the data source
     * @param clusterState clusterState
     * @return ClusterStateDto
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<ClusterStateDto> updateById(@PathVariable("id") String id, @RequestBody ClusterStateDto clusterState) {
        clusterState.setId(MongoUtils.toObjectId(id));
        return success(clusterStateService.save(clusterState, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     * @param fieldsJson fieldsJson
     * @return ClusterStateDto
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<ClusterStateDto> findById(@PathVariable("id") String id,
            @RequestParam("fields") String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(clusterStateService.findById(MongoUtils.toObjectId(id),  fields, getLoginUser()));
    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param clusterState clusterState
     * @return ClusterStateDto
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PutMapping("{id}")
    public ResponseMessage<ClusterStateDto> replceById(@PathVariable("id") String id, @RequestBody ClusterStateDto clusterState) {
        return success(clusterStateService.replaceById(MongoUtils.toObjectId(id), clusterState, getLoginUser()));
    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param clusterState clusterState
     * @return ClusterStateDto
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PostMapping("{id}/replace")
    public ResponseMessage<ClusterStateDto> replaceById2(@PathVariable("id") String id, @RequestBody ClusterStateDto clusterState) {
        return success(clusterStateService.replaceById(MongoUtils.toObjectId(id), clusterState, getLoginUser()));
    }



    /**
     * Delete a model instance by {{id}} from the data source
     * @param id id
     * @return boolean
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Boolean> delete(@PathVariable("id") String id) {
        if (settingsService.isCloud() || Boolean.TRUE.equals(permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_CLUSTER_MANAGEMENT, getLoginUser().getUserId()))) {
            return success(clusterStateService.deleteCluster(MongoUtils.toObjectId(id), getLoginUser()));
        } else {
            throw new BizException("NotAuthorized");
        }
    }

    /**
     *  Check whether a model instance exists in the data source
     * @param id id
     * @return map
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @GetMapping("{id}/exists")
    public ResponseMessage<HashMap<String, Boolean>> checkById1(@PathVariable("id") String id) {
        long count = clusterStateService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     *  Count instances of the model matched by where from the data source
     * @param whereJson whereJson
     * @return map
     */
    @Operation(summary = "Count instances of the model matched by where from the data source")
    @GetMapping("count")
    public ResponseMessage<HashMap<String, Long>> count(@RequestParam("where") String whereJson) {
        Where where = parseWhere(whereJson);
        if (where == null) {
            where = new Where();
        }
        long count = clusterStateService.count(where, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    /**
     *  Find first instance of the model matched by filter from the data source.
     * @param filterJson filterJson
     * @return ClusterStateDto
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<ClusterStateDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(clusterStateService.findOne(filter, getLoginUser()));
    }

    /**
     *  Update instances of the model matched by {{where}} from the data source.
     * @param whereJson whereJson
     * @return map
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody ClusterStateDto clusterState) {
        Where where = parseWhere(whereJson);
        long count = clusterStateService.updateByWhere(where, clusterState, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    /**
     *  Update an existing model instance or insert a new one into the data source based on the where criteria.
     * @param whereJson whereJson
     * @return ClusterStateDto
     */
    @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
    @PostMapping("upsertWithWhere")
    public ResponseMessage<ClusterStateDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody ClusterStateDto clusterState) {
        Where where = parseWhere(whereJson);
        return success(clusterStateService.upsertByWhere(where, clusterState, getLoginUser()));
    }


    /**
     * 更新agent版本
     * @return string
     */
    @Operation(summary = "更新agent版本")
    @PostMapping("updataAgent")
    public ResponseMessage<String> updateAgent(@RequestBody UpdateAgentVersionParam updateAgentVersionParam) {
       String result= clusterStateService.updateAgent(updateAgentVersionParam,getLoginUser());
        return success(result);
    }

    @Operation(summary = "update status")
    @PostMapping("/updataStatus")
    public ResponseMessage<Map<String, Long>> updataStatus(@RequestBody @Validated UpdataStatusRequest updataStatusRequest) {
        Long result = clusterStateService.updateStatus(updataStatusRequest, getLoginUser());
        return success(new HashMap<String, Long>(){{put("greeting", result);}});
    }

    @Operation(summary = "add monitor")
    @PostMapping("/addMonitor")
    public ResponseMessage<Map<String, Integer>> addMonitor(@RequestBody @Validated(ClusterStateMonitorRequest.ValidationType.AddMonitor.class) ClusterStateMonitorRequest editMonitorRequest) {
        if (settingsService.isCloud() || Boolean.TRUE.equals(permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_CLUSTER_MANAGEMENT, getLoginUser().getUserId()))) {
            Integer result = clusterStateService.addMonitor(editMonitorRequest,getLoginUser());
            return success(new HashMap<String, Integer>(){{put("status", result);}});
        }else{
            throw new BizException("NotAuthorized");
        }

    }

    @Operation(summary = "edit monitor")
    @PostMapping("/editMonitor")
    public ResponseMessage<Map<String, Integer>> editMonitor(@RequestBody @Validated(ClusterStateMonitorRequest.ValidationType.EditMonitor.class) ClusterStateMonitorRequest editMonitorRequest) {
        if (settingsService.isCloud() || Boolean.TRUE.equals(permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_CLUSTER_MANAGEMENT, getLoginUser().getUserId()))) {
            Integer result = clusterStateService.editMonitor(editMonitorRequest);
            return success(new HashMap<String, Integer>() {{
                put("status", result);
            }});
        } else {
            throw new BizException("NotAuthorized");
        }
    }

    @Operation(summary = "remove monitor")
    @PostMapping("/removeMonitor")
    public ResponseMessage<Map<String, Integer>> removeMonitor(@RequestBody @Validated(ClusterStateMonitorRequest.ValidationType.RemoveMonitor.class) ClusterStateMonitorRequest editMonitorRequest) {
        if (settingsService.isCloud() || Boolean.TRUE.equals(permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_CLUSTER_MANAGEMENT, getLoginUser().getUserId()))) {
            Integer result = clusterStateService.removeMonitor(editMonitorRequest, getLoginUser());
            return success(new HashMap<String, Integer>() {{
                put("status", result);
            }});
        } else {
            throw new BizException("NotAuthorized");
        }

    }

    @Operation(summary = "get simple flow engine info list")
    @GetMapping("/findAccessNodeInfo")
    public ResponseMessage<List<AccessNodeInfo>> findAccessNodeInfo () {
        return success(clusterStateService.findAccessNodeInfo(getLoginUser()));
    }

    @Operation(summary = "Find all raw server info")
    @GetMapping("/findRawServerInfo")
    public ResponseMessage<Page<RawServerStateDto>> findRawServerInfo(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        Page<RawServerStateDto> allLatest = rawServerStateService.getAllLatest(filter);
        for (RawServerStateDto item : allLatest.getItems()) {
            item.setServiceIP(null);
            item.setServicePort(null);
        }
        return success(allLatest);
    }

    @Operation(summary = "Execute Oracle Log Parser command, start/stop/restart")
    @PostMapping("/oracle-log-parser")
    public ResponseMessage<OracleLogParserCommandExecResult> executeCommand(
            @RequestParam(value = "serverId") String serverId,
            @RequestParam(value = "command") String command) {
        return success(oracleLogParserService.executeCommand(serverId, command));
    }


    @Operation(summary = "Upgrade Oracle Log Parser sn file")
    @PostMapping("/oracle-log-parser/upgrade-sn")
    public ResponseMessage<OracleLogParserUpgradeSNResult> upgradeOracleLogParserSN(
            @RequestParam(value = "serverId") String serverId,
            @RequestParam("file") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                throw new BizException("oracle.log.parser.sn.file.empty");
            }
            String contentType = file.getContentType();
            if (!isTextFile(contentType, file.getOriginalFilename())) {
                throw new BizException("oracle.log.parser.sn.file.not.text");
            }
            String content = new String(file.getBytes(), StandardCharsets.UTF_8);
            return success(oracleLogParserService.upgradeOracleLogParserSN(serverId, content));
        } catch (IOException e) {
            throw new BizException("oracle.log.parser.sn.file.upload.error", e.getMessage());
        }
    }

    private boolean isTextFile(String contentType, String filename) {
        // 检查常见的文本文件类型
        return contentType != null && (
                contentType.startsWith("text/") ||
                        contentType.equals("application/json") ||
                        contentType.equals("application/xml") ||
                        filename.endsWith(".txt") ||
                        filename.endsWith(".csv") ||
                        filename.endsWith(".json") ||
                        filename.endsWith(".xml")
        );
    }

    @Operation(summary = "Get Oracle Log Parser sn info")
    @GetMapping("/oracle-log-parser/sn")
    public ResponseMessage<OracleLogParserSNResult> findOracleLogParserSN(
            @RequestParam(value = "serverId") String serverId) {
        return success(oracleLogParserService.findOracleLogParserSN(serverId));
    }

    @Operation(summary = "Update Oracle Log Parser config")
    @PostMapping("/oracle-log-parser/update-config")
    public ResponseMessage<OracleLogParserUpdateConfigResult> updateOracleLogParserConfig(
            @RequestParam(value = "serverId") String serverId,
            @RequestBody OracleLogParserConfigParam parma) {
        return success(oracleLogParserService.updateOracleLogParserConfig(serverId, parma));
    }

    @Operation(summary = "remove Oracle Log Parser in list")
    @DeleteMapping("/oracle-log-parser")
    public ResponseMessage<Boolean> removeUselessServerInfo(
            @RequestParam(value = "serverId") String serverId) {
        return success(oracleLogParserService.removeUselessServerInfo(serverId));
    }
}
