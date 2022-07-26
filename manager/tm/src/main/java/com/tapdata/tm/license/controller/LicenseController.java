package com.tapdata.tm.license.controller;

import cn.hutool.json.JSONUtil;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.license.dto.LicenseDto;
import com.tapdata.tm.license.dto.LicenseUpdateDto;
import com.tapdata.tm.license.dto.LicenseUpdateReqDto;
import com.tapdata.tm.license.init.LicenseRunner;
import com.tapdata.tm.license.service.LicenseService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * @Date: 2021/12/06
 * @Description:
 */
@Tag(name = "License", description = "License相关接口")
@RestController
@RequestMapping("/api/Licenses")
@ApiResponses(value = {@ApiResponse(description = "successful operation", responseCode = "200")})
@Setter(onMethod_ = {@Autowired})
public class LicenseController extends BaseController {
    private LicenseService licenseService;

    /**
     *  Patch an existing model instance or insert a new one into the data source
     * @param license
     * @return
     */
//    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
//    @PatchMapping()
//    public ResponseMessage<LicenseDto> update(@RequestBody LicenseDto license) {
//        return success(licenseService.save(license, getLoginUser()));
//    }

    /**
     * Find all instances of the model matched by filter from the data source
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<LicenseDto>> page(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        Page<LicenseDto> page = licenseService.find(filter);
        List<LicenseDto> items = page.getItems();
        if (CollectionUtils.isNotEmpty(items)) {
            Map<String, Object> licenseMap = LicenseRunner.getLicenseMap();
            if (licenseMap != null) {
                Long expiresOn = (Long) licenseMap.get("expires_on");
                if (expiresOn != null) {
                    items.forEach(i-> {
                        if (i.getExpirationDate() == null) {
                            i.setExpirationDate(new Date(expiresOn));
                        }
                    });
                }
            }
        }
        return success(page);
    }

    /**
     *  Replace an existing model instance or insert a new one into the data source
     * @param license
     * @return
     */
//    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
//    @PutMapping
//    public ResponseMessage<LicenseDto> put(@RequestBody LicenseDto license) {
//        return success(licenseService.replaceOrInsert(license, getLoginUser()));
//    }

    /**
     * Create a new instance of the model and persist it into the data source
     * @param license
     * @return
     */
//    @Operation(summary = "Create a new instance of the model and persist it into the data source")
//    @PostMapping
//    public ResponseMessage<LicenseDto> save(@RequestBody LicenseDto license) {
//        license.setId(null);
//        return success(licenseService.save(license, getLoginUser()));
//    }


    /**
     *  Patch attributes for a model instance and persist it into the data source
     * @param license
     * @return
     */
//    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
//    @PatchMapping("{id}")
//    public ResponseMessage<LicenseDto> updateById(@PathVariable("id") String id, @RequestBody LicenseDto license) {
//        license.setId(MongoUtils.toObjectId(id));
//        return success(licenseService.save(license, getLoginUser()));
//    }

    /**
     * Find a model instance by {{id}} from the data source
     * @param fieldsJson
     * @return
     */
//    @Operation(summary = "Find a model instance by {{id}} from the data source")
//    @GetMapping("{id}")
//    public ResponseMessage<LicenseDto> findById(@PathVariable("id") String id,
//                                                @RequestParam(value = "fields", required = false) String fieldsJson) {
//        Field fields = parseField(fieldsJson);
//        return success(licenseService.findById(MongoUtils.toObjectId(id),  fields, getLoginUser()));
//    }

    /**
     * Check whether a model instance exists in the data source
     * @return
     */
//    @Operation(summary = "Check whether a model instance exists in the data source")
//    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
//    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
//        long count = licenseService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
//        HashMap<String, Boolean> existsValue = new HashMap<>();
//        existsValue.put("exists", count > 0);
//        return success(existsValue);
//    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param license
     * @return
     */
//    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
//    @PutMapping("{id}")
//    public ResponseMessage<LicenseDto> replaceById(@PathVariable("id") String id, @RequestBody LicenseDto license) {
//        return success(licenseService.replaceById(MongoUtils.toObjectId(id), license, getLoginUser()));
//    }

    /**
     * Delete a model instance by {{id}} from the data source
     * @param id
     * @return
     */
//    @Operation(summary = "Delete a model instance by {{id}} from the data source")
//    @DeleteMapping("{id}")
//    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
//        licenseService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
//        return success();
//    }

    /**
     *  Check whether a model instance exists in the data source
     * @param id
     * @return
     */
//    @Operation(summary = "Check whether a model instance exists in the data source")
//    @GetMapping("{id}/exists")
//    public ResponseMessage<HashMap<String, Boolean>> checkById1(@PathVariable("id") String id) {
//        long count = licenseService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
//        HashMap<String, Boolean> existsValue = new HashMap<>();
//        existsValue.put("exists", count > 0);
//        return success(existsValue);
//    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param license
     * @return
     */
//    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
//    @PostMapping("{id}/replace")
//    public ResponseMessage<LicenseDto> replaceById2(@PathVariable("id") String id, @RequestBody LicenseDto license) {
//        return success(licenseService.replaceById(MongoUtils.toObjectId(id), license, getLoginUser()));
//    }


//    @GetMapping("change-stream")
//    public ResponseMessage<Map<String, Long>> getChangeStream(@RequestParam("where") String whereJson, @RequestBody LicenseDto license) {
//
//        //todo
//        return success();
//    }
//
//    @PostMapping("change-stream")
//    public ResponseMessage<Map<String, Long>> changeStream(@RequestParam("where") String whereJson, @RequestBody LicenseDto license) {
//
//        //todo
//        return success();
//    }

    /**
     *  Count instances of the model matched by where from the data source
     * @param whereJson
     * @return
     */
//    @Operation(summary = "Count instances of the model matched by where from the data source")
//    @GetMapping("count")
//    public ResponseMessage<HashMap<String, Long>> count(@RequestParam("where") String whereJson) {
//        Where where = parseWhere(whereJson);
//        if (where == null) {
//            where = new Where();
//        }
//        long count = licenseService.count(where, getLoginUser());
//        HashMap<String, Long> countValue = new HashMap<>();
//        countValue.put("count", count);
//        return success(countValue);
//    }

    @GetMapping("/expires")
    public ResponseMessage<Map<String, Object>> expires(){
        return success(LicenseRunner.getLicenseMap());
    }

    /**
     *  Find first instance of the model matched by filter from the data source.
     * @param filterJson
     * @return
     */
//    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
//    @GetMapping("findOne")
//    public ResponseMessage<LicenseDto> findOne(
//            @Parameter(in = ParameterIn.QUERY,
//                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
//            )
//            @RequestParam(value = "filter", required = false) String filterJson) {
//        Filter filter = parseFilter(filterJson);
//        if (filter == null) {
//            filter = new Filter();
//        }
//        return success(licenseService.findOne(filter, getLoginUser()));
//    }

//    @GetMapping("period")
//    public ResponseMessage<String> period(String id) {
//        //todo
//        return success("countValue");
//    }

//    @PostMapping("replaceOrCreate")
//    public ResponseMessage<Map<String, Long>> replaceOrCreate(@RequestParam("where") String whereJson, @RequestBody LicenseDto license) {
//        Where where = parseWhere(whereJson);
//        long count = licenseService.updateByWhere(where, license, getLoginUser());
//        HashMap<String, Long> countValue = new HashMap<>();
//        countValue.put("count", count);
//        return success(countValue);
//    }

    @GetMapping("sid")
    public ResponseMessage<Map<String, String>> getSid(String id) {
        if (StringUtils.isEmpty(id)) {
            return failed(new BizException("IllegalArgument", "id"));
        }
        List<String> ids = JSONUtil.toList(id, String.class);
        List<LicenseDto> list = licenseService.getLicensesBySids(ids);
        String sid = list.stream().map(LicenseDto::getSid).collect(Collectors.joining("-"));

        Map<String, String> map = new HashMap<String, String>() {{
            put("sid", sid);
        }};
        return success(map);
    }

    /**
     *  Update instances of the model matched by {{where}} from the data source.
     * @param whereJson
     * @return
     */
//    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
//    @PostMapping("update")
//    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody LicenseDto license) {
//        Where where = parseWhere(whereJson);
//        long count = licenseService.updateByWhere(where, license, getLoginUser());
//        HashMap<String, Long> countValue = new HashMap<>();
//        countValue.put("count", count);
//        return success(countValue);
//    }

    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("upload")
    public ResponseMessage<String> upload(@RequestBody LicenseUpdateReqDto reqDto) {

        if (reqDto == null || CollectionUtils.isEmpty(reqDto.getSid())) {
            return failed(new BizException("IllegalArgument", "sid"));
        }

        String license = reqDto.getLicense();
        if (StringUtils.isEmpty(license)) {
            return failed("Illegal.license");
        }

        LicenseUpdateDto licenseUpdateDto;
        try {
            licenseUpdateDto = JsonUtil.parseJson(licenseService.decryptLicense(license), LicenseUpdateDto.class);
        } catch (Throwable e) {
            return failed("Illegal.license");
        }

        licenseUpdateDto.setReqDto(reqDto);

        boolean isUpdate = licenseService.updateLicense(licenseUpdateDto);
        if (isUpdate) {
            return success("ok");
        } else {
            return failed("Sid.is.wrong");
        }
    }

    /**
     *  Update an existing model instance or insert a new one into the data source based on the where criteria.
     * @param whereJson
     * @return
     */
//    @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
//    @PostMapping("upsertWithWhere")
//    public ResponseMessage<LicenseDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody LicenseDto license) {
//        Where where = parseWhere(whereJson);
//        return success(licenseService.upsertByWhere(where, license, getLoginUser()));
//    }

}