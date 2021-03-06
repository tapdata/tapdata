package com.tapdata.tm.metadatainstance.controller;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateUtil;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.commons.schema.bean.Table;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.metadatainstance.param.ClassificationParam;
import com.tapdata.tm.metadatainstance.param.TablesSupportInspectParam;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.metadatainstance.vo.*;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.utils.GZIPUtil;
import com.tapdata.tm.utils.MetadataUtil;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.tapdata.entity.schema.TapTable;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.BindException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;


/**
 * @Author: Zed
 * @Date: 2021/09/11
 * @Description:
 */
@Tag(name = "MetadataInstances", description = "???????????????????????????")
@RestController
@RequestMapping("api/MetadataInstances")
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class MetadataInstancesController extends BaseController {
    private MetadataInstancesService metadataInstancesService;
    private ModulesService modulesService;
    private InspectService inspectService;

    /**
     * ???????????????
     */
    @Operation(summary = "???????????????")
    @PostMapping
    public ResponseMessage<MetadataInstancesDto> save(@RequestBody MetadataInstancesDto metadataInstances) {
        metadataInstances.setId(null);
        return success(metadataInstancesService.save(metadataInstances, getLoginUser()));
    }

    /**
     * Patch an existing model instance or insert a new one into the data source
     *
     * @param metadataInstances metadataInstances
     * @return MetadataInstancesDto
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<MetadataInstancesDto> update(@RequestBody MetadataInstancesDto metadataInstances) {
        return success(metadataInstancesService.save(metadataInstances, getLoginUser()));
    }


    /**
     *
     */
    @Operation(summary = " ")
    @GetMapping
    public ResponseMessage<Page<MetadataInstancesDto>> find(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(metadataInstancesService.list(filter, getLoginUser()));
    }


    /**
     * ????????????????????????????????????????????????????????????????????????
     *
     * @param filterJson filter
     * @return List<MetadataInstancesVo>
     */
    @Operation(summary = "????????????????????????????????????????????????????????????????????????")
    @GetMapping("findInspect")
    public ResponseMessage<List<MetadataInstancesVo>> findInspect(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        List<MetadataInstancesVo> list = metadataInstancesService.findInspect(filter, getLoginUser());
        return success(list);
    }


    /**
     * findInspect  ???????????????????????????????????????get???????????????????????????post??????
     * @return List<MetadataInstancesVo>
     */
    @RequestMapping(value = "findInspectPost",
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseMessage<List<MetadataInstancesVo>> findInspectPost(HttpServletRequest request ) throws IOException {
        String filterJson = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        List<MetadataInstancesVo> list = metadataInstancesService.findInspect(filter, getLoginUser());
        return success(list);
    }

    @GetMapping("node/schema")
    public ResponseMessage<List<MetadataInstancesDto>> findByNodeId(@RequestParam("nodeId") String nodeId,
                                                                    @RequestParam(value = "fields", required = false) List<String> fields) {

        List<MetadataInstancesDto> metadataInstancesDtos = metadataInstancesService.findByNodeId(nodeId, fields, getLoginUser(), null);
        if (CollectionUtils.isNotEmpty(metadataInstancesDtos)) {
            for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtos) {
                ////??????????????????????????????
                MetadataInstancesDto.sortField(metadataInstancesDto.getFields());
            }
        }
        return success(metadataInstancesDtos);
    }

    @GetMapping("node/oldSchema")
    public ResponseMessage<List<Table>> findOldByNodeId(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        return success(metadataInstancesService.findOldByNodeId(filter, getLoginUser()));
    }

    @GetMapping("node/tableMap")
    public ResponseMessage<Map<String, String>> findTableMapByNodeId(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        return success(metadataInstancesService.findTableMapByNodeId(filter, getLoginUser()));
    }


    /**
     * Replace an existing model instance or insert a new one into the data source
     *
     * @param metadataInstances
     * @return
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<MetadataInstancesDto> put(@RequestBody MetadataInstancesDto metadataInstances) {
        return success(metadataInstancesService.replaceOrInsert(metadataInstances, getLoginUser()));
    }


    /**
     * Check whether a model instance exists in the data source
     *
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        long count = metadataInstancesService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     * ???????????????
     * ???????????????????????????
     */
    @Operation(summary = "???????????????")
    @PatchMapping("{id}")
    public ResponseMessage<MetadataInstancesDto> updateById(@PathVariable("id") String id, @RequestBody MetadataInstancesDto metadataInstances) {
        ObjectId objectId = MongoUtils.toObjectId(id);
        return success(metadataInstancesService.modifyById(objectId, metadataInstances, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     *
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<MetadataInstancesDto> findById(@PathVariable("id") String id,
                                                          @RequestParam(value = "fields", required = false) String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(metadataInstancesService.queryById(MongoUtils.toObjectId(id), fields, getLoginUser()));
    }


    /**
     * ???????????????????????????
     *
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("findTablesById/{id}")
    public ResponseMessage<TableListVo> findTablesById(@PathVariable("id") String id,
                                                       @RequestParam(value = "fields", required = false) String fieldsJson) {
        return success(metadataInstancesService.findTablesById(id));
    }

    /**
     * Replace attributes for a model instance and persist it into the data source.
     *
     * @param metadataInstances
     * @return
     */
    @Deprecated
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PutMapping("{id}")
    public ResponseMessage<MetadataInstancesDto> replceById(@PathVariable("id") String id, @RequestBody MetadataInstancesDto metadataInstances) {
        return success(metadataInstancesService.replaceById(MongoUtils.toObjectId(id), metadataInstances, getLoginUser()));
    }

    /**
     * Replace attributes for a model instance and persist it into the data source.
     *
     * @param metadataInstances
     * @return
     */
    @Deprecated
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PostMapping("{id}/replace")
    public ResponseMessage<MetadataInstancesDto> replaceById2(@PathVariable("id") String id, @RequestBody MetadataInstancesDto metadataInstances) {
        return success(metadataInstancesService.replaceById(MongoUtils.toObjectId(id), metadataInstances, getLoginUser()));
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
        metadataInstancesService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
        return success();
    }

    /**
     * Check whether a model instance exists in the data source
     *
     * @param id
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @GetMapping("{id}/exists")
    public ResponseMessage<HashMap<String, Boolean>> checkById1(@PathVariable("id") String id) {
        long count = metadataInstancesService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     * Count instances of the model matched by where from the data source
     *
     * @param whereJson
     * @return
     */
    @Operation(summary = "Count instances of the model matched by where from the data source")
    @GetMapping("count")
    public ResponseMessage<HashMap<String, Long>> count(@RequestParam("where") String whereJson) {
        Where where = parseWhere(whereJson);
        if (where == null) {
            where = new Where();
        }
        long count = metadataInstancesService.count(where, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    /**
     * Find first instance of the model matched by filter from the data source.
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<MetadataInstancesDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(metadataInstancesService.queryByOne(filter, getLoginUser()));
    }

    /**
     * Update instances of the model matched by {{where}} from the data source.
     *
     * @param whereJson where
     * @return map
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody String reqBody) {
        Where where = parseWhere(whereJson);
        UserDetail user = getLoginUser();
        Document body = Document.parse(reqBody);
        if (!body.containsKey("$set") && !body.containsKey("$setOnInsert") && !body.containsKey("$unset")) {
            Document _body = new Document();
            _body.put("$set", body);
            body = _body;
        }
        long count = metadataInstancesService.updateByWhere(where, body, user);

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
    public ResponseMessage<MetadataInstancesDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody MetadataInstancesDto metadataInstances) {
        Where where = parseWhere(whereJson);
        return success(metadataInstancesService.upsertByWhere(where, metadataInstances, getLoginUser()));
    }


    /**
     * ??????jobstat
     *
     * @param skip
     * @param limit
     * @return
     */
    @Operation(summary = "query job stats")
    @GetMapping("jobStats")
    public ResponseMessage<List<MetadataInstancesDto>> jobStats(@RequestParam(value = "skip", required = false, defaultValue = "0") long skip,
                                                                @RequestParam(value = "limit", required = false, defaultValue = "20") int limit) {
        return success(metadataInstancesService.jobStats(skip, limit));
    }


    @Operation(summary = "query schema")
    @GetMapping("schema")
    public ResponseMessage<List<MetadataInstancesDto>> schema(@RequestParam("filter") String filterJson) {
        Filter filter = parseFilter(filterJson);
        return success(metadataInstancesService.schema(filter, getLoginUser()));
    }

    @GetMapping("{id}/lienage")
    public ResponseMessage<List<MetadataInstancesDto>> lienage(@PathVariable("id") String id) {
        return success(metadataInstancesService.lienage(id));
    }

    @GetMapping("compareHistory")
    public ResponseMessage<MetadataUtil.CompareResult> compareHistory(@RequestParam("id") String id, @RequestParam("historyVersion") int historyVersion) {
        return success(metadataInstancesService.compareHistory(MongoUtils.toObjectId(id), historyVersion));
    }

    @GetMapping("tableConnection")
    public ResponseMessage<List<MetadataInstancesDto>> tableConnection(@RequestParam("name") String name) {
        return success(metadataInstancesService.tableConnection(name, getLoginUser()));
    }

    /**
     * @param map
     * @return
     */
    @PatchMapping("classifications")
    public ResponseMessage<Map<String, Object>> classifications(@RequestBody Map<String, List<ClassificationParam>> map) {
        List<ClassificationParam> classificationParamList = map.get("metadatas");
        return success(metadataInstancesService.classifications(classificationParamList));
    }


    /**
     * ??????????????????????????????????????????????????????????????????
     * isTarget ???????????????
     *
     * @return
     */
    @GetMapping("originalData")
    public ResponseMessage<List<MetadataInstancesDto>> originalData(@RequestParam(value = "isTarget", required = false) String isTarget,
                                                                    @RequestParam("qualified_name") String qualified_name) {
        return success(metadataInstancesService.originalData(isTarget, qualified_name, getLoginUser()));
    }

    @GetMapping("tables")
    public ResponseMessage<List<String>> tables(String connectionId, @RequestParam(value = "sourceType", defaultValue = "SOURCE")String sourceType) {
        return success(metadataInstancesService.tables(connectionId, sourceType));
    }


    /**
     * ???????????????????????????????????????
     * @param connectionId
     * @return
     */
    @GetMapping("tableSupportInspect")
    public ResponseMessage<TableSupportInspectVo> tableSupportInspect(String connectionId, String tableName) {
        return success(metadataInstancesService.tableSupportInspect(connectionId,tableName));
    }

    /**
     * ???????????????????????????????????????
     * @param tablesSupportInspectParam
     * @return
     */
    @PostMapping("tablesSupportInspect")
    public ResponseMessage<List<TableSupportInspectVo>> tablesSupportInspect(@RequestBody TablesSupportInspectParam tablesSupportInspectParam) {
        return success(metadataInstancesService.tablesSupportInspect(tablesSupportInspectParam));
    }

    @GetMapping("metadata")
    public ResponseMessage<Table> getMetadata(@RequestParam("connectionId") String connectionId
            , @RequestParam("metaType") String metaType, @RequestParam("tableName") String tableName) {
        return success(metadataInstancesService.getMetadata(connectionId, metaType, tableName, getLoginUser()));
    }

    @GetMapping("metadata/v2")
    public ResponseMessage<TapTable> getMetadatav2(@RequestParam("connectionId") String connectionId
            , @RequestParam("metaType") String metaType, @RequestParam("tableName") String tableName) {
        return success(metadataInstancesService.getMetadataV2(connectionId, metaType, tableName, getLoginUser()));
    }


    @GetMapping("tapTables")
    public ResponseMessage<Page<TapTable>> getTapTable(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(metadataInstancesService.getTapTable(filter, getLoginUser()));
    }

    @GetMapping("search")
    public ResponseMessage<List<Map<String, Object>>> search(@RequestParam("type") String type
            , @RequestParam("keyword") String keyword
            , @RequestParam(value = "lastId", required = false) String lastId
            , @RequestParam(value = "pageSize", required = false, defaultValue = "15") Integer pageSize) {
        return success(metadataInstancesService.search(type, keyword, lastId, pageSize, getLoginUser()));
    }

    /**
     *  ??????????????????????????? ????????????????????????????????????????????????????????????????????????????????????/tables????????????????????????
     * @param connectionId
     * @param keyword
     * @param lastId
     * @param pageSize
     * @return
     */
    @GetMapping("tableSearch")
    @Operation(summary = "???????????????-?????????????????????")
    @Deprecated
    public ResponseMessage<List<MetaTableVo>> tableSearch(@RequestParam("connectionId") String connectionId
            , @RequestParam("keyword") String keyword
            , @RequestParam(value = "lastId", required = false) String lastId
            , @RequestParam(value = "pageSize", required = false, defaultValue = "100") Integer pageSize) {
        return success(metadataInstancesService.tableSearch(connectionId, keyword, lastId, pageSize, getLoginUser()));
    }

    /**
     * ?????????????????????????????? ?????????????????????????????????
     * @param connectionId
     * @param names
     * @return
     */
    @GetMapping("checkNames")
    @Operation(summary = "???????????????-?????????????????????")
    public ResponseMessage<MetaTableCheckVo> checkTableNames(@RequestParam("connectionId") String connectionId, @RequestParam("names") List<String> names) {
        return success(metadataInstancesService.checkTableNames(connectionId, names, getLoginUser()));
    }


    /**
     * ????????????
     * {"_id":{"in":["61519f8ed51f7400c71cfdc1","6155699428f0ea0052de4482","61610d7c8ada020054d1226d","61a49db2728c0100ad04c049","61a49f4e728c0100ad04c14c","6131c368a4a5140052ae130e","61a49d67728c0100ad04c018","61a49d6b728c0100ad04c01d","619f3dd47e7bfb737e8c369f","61a590dc728c0100ad055c3a","6151918c562c8a0052ff1f07","6151b142d51f7400c71d2bea","615424a20e5b5800dbcb4dde","6131d317a4a5140052ae1eed"]}}
     *
     * @param filterJson
     * @param request
     * @param response
     * @return
     */
  /*  @PostMapping("download")
    public ResponseMessage download(@RequestParam(value = "filter", required = false) String filterJson, HttpServletRequest request, HttpServletResponse response) throws IOException {
        Where where = parseWhere(filterJson);
        Map jsonObject = (Map) where.get("_id");
        List idList = (List) jsonObject.get("in");

        Query query = Query.query(Criteria.where("id").in(idList));
        // ??????????????????
        GridFSFindIterable gridFSFiles = gridFsTemplate.findOne(query);

        if (gridFSFiles == null) {
            return failed("");
        }

        gridFSFiles.

                String fileName = gfsfile.getFilename().replace(",", "");
        //???????????????????????????
        if (request.getHeader("User-Agent").toUpperCase().contains("MSIE") ||
                request.getHeader("User-Agent").toUpperCase().contains("TRIDENT")
                || request.getHeader("User-Agent").toUpperCase().contains("EDGE")) {
            fileName = java.net.URLEncoder.encode(fileName, "UTF-8");
        } else {
            //???IE?????????????????????
            fileName = new String(fileName.getBytes("UTF-8"), "ISO-8859-1");
        }
        // ?????????????????????????????????
        response.setContentType(gfsfile.getContentType());
        response.setHeader("Content-Disposition", "attachment;filename=\"" + "222.png" + "\"");
        gfsfile.writeTo(response.getOutputStream());
    }*/

    /**
     * @param response a HttpServletResponse
     */

    /**
     * @param response a HttpServletResponse
     */
    @GetMapping(value = "/download")
    public void downloadFile(@RequestParam(value = "type", required = false) String type,
                             @RequestParam(value = "where", required = false) String whereJson,
                             HttpServletResponse response) {
        Where where = parseWhere(whereJson);
        Map jsonObject = (Map) where.get("_id");
        List idList = (List) jsonObject.get("$in");

        Query query = Query.query(Criteria.where("id").in(idList));
        Map<String, Object> data = new HashMap();
        if ("Modules".equals(type)) {
            List<ModulesDto> modulesDtoList = modulesService.findAll(query);

            List<ExportModulesVo> exportModulesVoList=new ArrayList<>();
            data.put("collection", "Modules");
            if (CollectionUtils.isNotEmpty(modulesDtoList)) {
                for (ModulesDto modulesDto : modulesDtoList) {
                    ExportModulesVo exportModulesVo= BeanUtil.copyProperties(modulesDto,ExportModulesVo.class);
                    if (null!=modulesDto.getConnection()){
                        exportModulesVo.setConnection(modulesDto.getConnection().toString());
                    }
                    exportModulesVo.setUser_id(modulesDto.getUserId());
                    exportModulesVoList.add(exportModulesVo);
                }
            }
            data.put("data", exportModulesVoList);
        } else if ("Inspect".equals(type)) {
            List inspectDtoList = inspectService.findAll(query);
            data.put("collection", "Inspect");
            data.put("data", inspectDtoList);
        }

        String downloadContent = JsonUtil.toJsonUseJackson(data);

        String fileName = type + DateUtil.today() + ".gz";
        response.setHeader("Content-Disposition", "attachment; filename=" + fileName);
        ServletOutputStream outputStream = null;
        try {
            outputStream = response.getOutputStream();
            outputStream.write(GZIPUtil.gzip(downloadContent.getBytes(StandardCharsets.UTF_8)));

        } catch (IOException ioe) {
            log.error("????????????", ioe);
        } finally {
            try {
                response.flushBuffer();
                if (null != outputStream) {
                    outputStream.close();
                }
            } catch (IOException e) {
                log.error("?????????????????????", e);
            }
        }
    }

    /**
     * @param request upsert  0??????????????????  1 ??????????????????
     *                listtags=[{%22id%22:%2261a4c74b728c0100ad04dc07%22,%22value%22:%22category2%22}]&type=APIServer
     * @return
     * @desc ???????????????
     */
    @PostMapping("/upload")
    public ResponseMessage upload(HttpServletRequest request,
                                  @RequestParam(value = "upsert") String upsert,
                                  @RequestParam(value = "type", required = false) String type,
                                  @RequestParam(value = "listtags", required = false) List listtags) throws IOException {
        MultipartHttpServletRequest multipartRequest = (MultipartHttpServletRequest) request;
        MultipartFile file = multipartRequest.getFile("file");

        if (null == file) {
            log.error("??????????????????");
            throw new BindException("Upload.File.NotExist");
        }

        byte[] bytes = GZIPUtil.unGzip(file.getBytes());
        String json = new String(bytes);

        if ("Modules".equals(type)) {
            modulesService.importData(json, upsert, listtags, getLoginUser());
        } else if ("Inspect".equals(type)) {
            inspectService.importData(json, upsert, getLoginUser());
        }
        return success();
    }


    @GetMapping("mergerNode/parent/fields")
    public ResponseMessage<List<com.tapdata.tm.commons.schema.Field>> mergeNodeParentField(@RequestParam("taskId") String taskId,
                                                                                                  @RequestParam("nodeId") String nodeId) {
        return success(metadataInstancesService.getMergeNodeParentField(taskId, nodeId, getLoginUser()));

    }


}
