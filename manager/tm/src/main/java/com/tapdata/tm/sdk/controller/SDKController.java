package com.tapdata.tm.sdk.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.sdk.dto.SDKDto;
import com.tapdata.tm.sdk.service.SDKService;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import jakarta.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;


/**
 * @Date: 2025/07/01
 * @Description:
 */
@Tag(name = "SDK", description = "SDK Manager相关接口")
@RestController
@RequestMapping("/api/SDK")
public class SDKController extends BaseController {

    private final SDKService sDKService;

    private final FileService fileService;

    public SDKController(SDKService sDKService, FileService fileService) {
        this.sDKService = sDKService;
        this.fileService = fileService;
    }

    /**
     * Create a new instance of the model and persist it into the data source
     * @param sDK
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<SDKDto> save(@RequestBody SDKDto sDK) {
        sDK.setId(null);
        return success(sDKService.save(sDK, getLoginUser()));
    }

    /**
     *  Patch an existing model instance or insert a new one into the data source
     * @param sDK
     * @return
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<SDKDto> update(@RequestBody SDKDto sDK) {
        return success(sDKService.save(sDK, getLoginUser()));
    }


    /**
     * Find all instances of the model matched by filter from the data source
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<SDKDto>> find(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(sDKService.find(filter, getLoginUser()));
    }

    /**
     *  Replace an existing model instance or insert a new one into the data source
     * @param sDK
     * @return
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<SDKDto> put(@RequestBody SDKDto sDK) {
        return success(sDKService.replaceOrInsert(sDK, getLoginUser()));
    }


    /**
     * Check whether a model instance exists in the data source
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        long count = sDKService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     *  Patch attributes for a model instance and persist it into the data source
     * @param sDK
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<SDKDto> updateById(@PathVariable("id") String id, @RequestBody SDKDto sDK) {
        sDK.setId(MongoUtils.toObjectId(id));
        return success(sDKService.save(sDK, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<SDKDto> findById(@PathVariable("id") String id,
            @RequestParam(value = "fields", required = false) String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(sDKService.findById(MongoUtils.toObjectId(id),  fields, getLoginUser()));
    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param sDK
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PutMapping("{id}")
    public ResponseMessage<SDKDto> replceById(@PathVariable("id") String id, @RequestBody SDKDto sDK) {
        return success(sDKService.replaceById(MongoUtils.toObjectId(id), sDK, getLoginUser()));
    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param sDK
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PostMapping("{id}/replace")
    public ResponseMessage<SDKDto> replaceById2(@PathVariable("id") String id, @RequestBody SDKDto sDK) {
        return success(sDKService.replaceById(MongoUtils.toObjectId(id), sDK, getLoginUser()));
    }



    /**
     * Delete a model instance by {{id}} from the data source
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        sDKService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
        return success();
    }

    /**
     *  Check whether a model instance exists in the data source
     * @param id
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @GetMapping("{id}/exists")
    public ResponseMessage<HashMap<String, Boolean>> checkById1(@PathVariable("id") String id) {
        long count = sDKService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     *  Count instances of the model matched by where from the data source
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
        long count = sDKService.count(where, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    /**
     *  Find first instance of the model matched by filter from the data source.
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<SDKDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(sDKService.findOne(filter, getLoginUser()));
    }

    /**
     *  Update instances of the model matched by {{where}} from the data source.
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody SDKDto sDK) {
        Where where = parseWhere(whereJson);
        long count = sDKService.updateByWhere(where, sDK, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    /**
     *  Update an existing model instance or insert a new one into the data source based on the where criteria.
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
    @PostMapping("upsertWithWhere")
    public ResponseMessage<SDKDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody SDKDto sDK) {
        Where where = parseWhere(whereJson);
        return success(sDKService.upsertByWhere(where, sDK, getLoginUser()));
    }

    /**
     * Download ZIP or JAR file by GridFS ID
     * @param gridfsId GridFS file ID
     * @param response HTTP response
     * @return ResponseMessage<Void>
     */
    @Operation(summary = "Download ZIP or JAR file by GridFS ID")
    @GetMapping(value = "/download/{gridfsId}", produces = {MediaType.APPLICATION_OCTET_STREAM_VALUE})
    public ResponseMessage<Void> downloadFile(
            @Parameter(description = "GridFS file ID") @PathVariable("gridfsId") String gridfsId, HttpServletResponse response) {

        if (StringUtils.isBlank(gridfsId)) {
            throw new IllegalArgumentException("GridFS ID cannot be empty");
        }

        ObjectId fileId = MongoUtils.toObjectId(gridfsId);
        if (fileId == null) {
            throw new IllegalArgumentException("Invalid GridFS ID format");
        }
        fileService.viewImg(fileId, response);
        return success();
    }

}