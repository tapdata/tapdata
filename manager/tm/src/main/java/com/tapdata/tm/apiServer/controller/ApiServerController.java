package com.tapdata.tm.apiServer.controller;

import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.apiServer.dto.ApiServerDto;
import com.tapdata.tm.apiServer.service.ApiServerService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
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

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;


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
        return success(apiServerService.find(filter,getLoginUser()));
    }


    /**
     * Patch attributes for a model instance and persist it into the data source
     *
     * @param metadataDefinition
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping
    public ResponseMessage<ApiServerDto> updateById(@RequestBody ApiServerDto metadataDefinition) {
        return success(apiServerService.updateById(metadataDefinition, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     *
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<ApiServerDto> findById(@PathVariable("id") String id,
                                                    @RequestParam("fields") String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(apiServerService.findById(MongoUtils.toObjectId(id), fields, getLoginUser()));
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
        apiServerService.deleteLogicsById(id);
        return success();
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
        return success(apiServerService.findOne(filter, getLoginUser()));
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
        long count = apiServerService.updateByWhere(where, metadataDefinition, getLoginUser());
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
        return success(apiServerService.upsertByWhere(where, metadataDefinition, getLoginUser()));
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