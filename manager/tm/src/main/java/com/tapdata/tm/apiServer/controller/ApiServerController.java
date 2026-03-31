package com.tapdata.tm.apiServer.controller;

import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.apiServer.dto.ApiServerDto;
import com.tapdata.tm.apiServer.service.ApiServerService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.utils.GZIPUtil;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.*;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;


/**
 * @Date: 2021/10/15
 * @Description:
 */
@Tag(name = "ApiServers", description = "ApiServers 相关接口")
@RestController
@Slf4j
@RequestMapping(value = {"/api/ApiServers"})
public class ApiServerController extends BaseController {

    @Autowired
    private ApiServerService apiServerService;

    private <T> T dataPermissionUnAuth() {
        throw new RuntimeException("Un auth");
    }

    private <T> T dataPermissionCheckOfMenu(UserDetail userDetail, DataPermissionActionEnums actionEnums, Supplier<T> supplier) {
        return DataPermissionHelper.check(userDetail, DataPermissionMenuEnums.ApiServers, actionEnums, DataPermissionDataTypeEnums.ApiServer, null, supplier, this::dataPermissionUnAuth);
    }

    private <T> T dataPermissionCheckOfId(HttpServletRequest request, UserDetail userDetail, String id, DataPermissionActionEnums actionEnums, Supplier<T> supplier) {
        String signId = id;
        if (request != null) {
            signId = Optional.ofNullable(DataPermissionHelper.signDecode(request, id)).orElse(id);
        }
        return DataPermissionHelper.checkOfQuery(
                userDetail,
                DataPermissionDataTypeEnums.ApiServer,
                actionEnums,
                apiServerService.dataPermissionFindById(MongoUtils.toObjectId(signId), new Field()),
                dto -> DataPermissionMenuEnums.ApiServers,
                supplier,
                this::dataPermissionUnAuth
        );
    }


    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<ApiServerDto> save(@RequestBody ApiServerDto metadataDefinition) {
        metadataDefinition.setId(null);
        return success(apiServerService.save(metadataDefinition, getLoginUser()));
    }

    /**
     * 分页返回
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<ApiServerDto>> find(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        Filter finalFilter = filter;
        UserDetail userDetail = getLoginUser();
        return success(dataPermissionCheckOfMenu(userDetail,DataPermissionActionEnums.View,
                () -> apiServerService.find(finalFilter, userDetail)));
    }


    /**
     * Patch attributes for a model instance and persist it into the data source
     *
     * @param metadataDefinition
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping
    public ResponseMessage<ApiServerDto> updateById(HttpServletRequest request, @RequestBody ApiServerDto metadataDefinition) {
        UserDetail userDetail = getLoginUser();
        return success(dataPermissionCheckOfId(request, userDetail, metadataDefinition.getId().toHexString(), DataPermissionActionEnums.Edit,
                () -> apiServerService.updateById(metadataDefinition, userDetail)));
    }

    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<ApiServerDto> updateById(HttpServletRequest request, @PathVariable("id") String id, @RequestBody ApiServerDto metadataDefinition) {
        if (metadataDefinition.getId() == null || !metadataDefinition.getId().toHexString().equals(id)) {
            metadataDefinition.setId(MongoUtils.toObjectId(id));
        }
        UserDetail userDetail = getLoginUser();
        return success(dataPermissionCheckOfId(request, userDetail, id, DataPermissionActionEnums.Edit,
                () -> apiServerService.updateById(metadataDefinition, userDetail)));
    }


    /**
     * Find a model instance by {{id}} from the data source
     *
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<ApiServerDto> findById(HttpServletRequest request, @PathVariable("id") String id,
                                                    @RequestParam("fields") String fieldsJson) {
        Field fields = parseField(fieldsJson);
        UserDetail userDetail = getLoginUser();
        return success(dataPermissionCheckOfId(request, userDetail, id, DataPermissionActionEnums.View,
                () -> apiServerService.findById(MongoUtils.toObjectId(id), fields, userDetail)));
    }


    /**
     * Delete a model instance by {{id}} from the data source
     *
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(HttpServletRequest request, @PathVariable("id") String id) {
        UserDetail userDetail = getLoginUser();
        return success(dataPermissionCheckOfId(request, userDetail, id, DataPermissionActionEnums.Delete, () -> {
            apiServerService.deleteLogicsById(id);
            return null;
        }));
    }


    /**
     *  Count instances of the model matched by where from the data source
     * @param whereJson
     * @return
     */
  /*  @Operation(summary = "Count instances of the model matched by where from the data source")
    @GetMapping("count")
    public ResponseMessage<HashMap<String, Long>> count(@RequestParam("where") String whereJson) {
        Where where = parseWhere(whereJson);
        if (where == null) {
            where = new Where();
        }
        long count = apiServerService.count(where, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }*/

    /**
     * Find first instance of the model matched by filter from the data source.
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<ApiServerDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        Filter finalFilter = filter;
        UserDetail userDetail = getLoginUser();
        return success(dataPermissionCheckOfMenu(userDetail, DataPermissionActionEnums.View,
                () -> apiServerService.findOne(finalFilter, userDetail)));
    }

    /**
     * Update instances of the model matched by {{where}} from the data source.
     *
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody ApiServerDto metadataDefinition) {
        Where where = parseWhere(whereJson);
        long count = dataPermissionCheckOfMenu(getLoginUser(), DataPermissionActionEnums.Edit,
                () -> apiServerService.updateByWhere(where, metadataDefinition, getLoginUser()));
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
    public ResponseMessage<ApiServerDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody ApiServerDto metadataDefinition) {
        Where where = parseWhere(whereJson);
        return success(dataPermissionCheckOfMenu(getLoginUser(), DataPermissionActionEnums.Edit,
                () -> apiServerService.upsertByWhere(where, metadataDefinition, getLoginUser())));
    }


    /**
     * txt文件下载接口
     * 1. 将字符串写入到文件中提供下载
     *
     * @param response a HttpServletResponse
     */
    @GetMapping(value = "/download/{id}")
    public void downloadFile(@PathVariable("id") String id, HttpServletResponse response) {
        Query query = Query.query(Criteria.where("id").is(id));
        ApiServerDto apiServerDto = apiServerService.findOne(query);

//        String metadataInstanceStr = JsonUtil.toJson(metadataInstance);
        Map data = new HashMap();
        data.put("collection", "ApiServer");
        data.put("data", apiServerDto);

        String downloadContent = JsonUtil.toJsonUseJackson(data);

        String fileName = DateUtil.today() + ".gz";
        response.setHeader("Content-Disposition", "attachment; filename=" + fileName);
        ServletOutputStream outputStream = null;
        try {
            outputStream = response.getOutputStream();
            outputStream.write(GZIPUtil.gzip(downloadContent.getBytes(StandardCharsets.UTF_8)));

        } catch (IOException ioe) {
            log.error("下载出错", ioe);
        } finally {
            try {
                response.flushBuffer();
                if (null != outputStream) {
                    outputStream.close();
                }
            } catch (IOException e) {
                log.error("下载关闭流出错", e);
            }
        }
    }

}