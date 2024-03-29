/**
 * @title: RoleMappingController
 * @description:
 * @author lk
 * @date 2021/12/6
 */
package com.tapdata.tm.roleMapping.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/RoleMappings")
@Slf4j
@Tag(name = "RoleMapping", description = "RoleMapping接口")
@ApiResponses(value = {@ApiResponse(description = "successful operation", responseCode = "200")})
public class RoleMappingController extends BaseController {

    private final RoleMappingService roleMappingService;

    public RoleMappingController(RoleMappingService roleMappingService) {
        this.roleMappingService = roleMappingService;
    }

    /**
     * Create a new instance of the model and persist it into the data source
     *
     * @param roleDto
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<RoleMappingDto> save(@RequestBody RoleMappingDto roleDto) {
        roleDto.setId(null);
        return success(roleMappingService.save(roleDto, getLoginUser()));
    }

    /**
     * Create a new instance of the model and persist it into the data source
     *
     * @param roleDtos
     * @return
     */
    @Operation(summary = "RoleMappings saveAll")
    @PostMapping("/saveAll")
    public ResponseMessage<List<RoleMappingDto>> saveAll(@RequestBody List<RoleMappingDto> roleDtos) {
//		return success(roleDtos.stream().map(roleDto -> save(roleDto).getData()).collect(Collectors.toList()));
        if (CollectionUtils.isNotEmpty(roleDtos)){
            UserDetail userDetail=getLoginUser();
            roleMappingService.saveAll(roleDtos,userDetail);
        }
        return success(roleDtos);
    }

    /**
     * Patch an existing model instance or insert a new one into the data source
     *
     * @param roleDto
     * @return
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<RoleMappingDto> update(@RequestBody RoleMappingDto roleDto) {
        return success(roleMappingService.save(roleDto, getLoginUser()));
    }


    /**
     * Find all instances of the model matched by filter from the data source
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage find(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        Page<RoleMappingDto> roleMappingDtos = roleMappingService.find(filter);

        Where where = filter.getWhere();
        if (null != where.get("roleId")) {
            String roleId = where.remove("roleId").toString();
            where.put("roleId", roleId);
        }
        List<RoleMappingDto> roleMappingDtos1 = roleMappingService.findAll(where);
        return success(roleMappingDtos1);
    }

    /**
     * Replace an existing model instance or insert a new one into the data source
     *
     * @param roleDto
     * @return
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<RoleMappingDto> put(@RequestBody RoleMappingDto roleDto) {
        return success(roleMappingService.replaceOrInsert(roleDto, getLoginUser()));
    }


    /**
     * Check whether a model instance exists in the data source
     *
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        long count = roleMappingService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     * Patch attributes for a model instance and persist it into the data source
     *
     * @param roleDto
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<RoleMappingDto> updateById(@PathVariable("id") String id, @RequestBody RoleMappingDto roleDto) {
        roleDto.setId(MongoUtils.toObjectId(id));
        return success(roleMappingService.save(roleDto, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     *
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<RoleMappingDto> findById(@PathVariable("id") String id,
                                                    @RequestParam("fields") String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(roleMappingService.findById(MongoUtils.toObjectId(id), fields));
    }

    /**
     * Replace attributes for a model instance and persist it into the data source.
     *
     * @param roleDto
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PutMapping("{id}")
    public ResponseMessage<RoleMappingDto> replceById(@PathVariable("id") String id, @RequestBody RoleMappingDto roleDto) {
        return success(roleMappingService.replaceById(MongoUtils.toObjectId(id), roleDto, getLoginUser()));
    }

    /**
     * Replace attributes for a model instance and persist it into the data source.
     *
     * @param roleDto
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PostMapping("{id}/replace")
    public ResponseMessage<RoleMappingDto> replaceById2(@PathVariable("id") String id, @RequestBody RoleMappingDto roleDto) {
        return success(roleMappingService.replaceById(MongoUtils.toObjectId(id), roleDto, getLoginUser()));
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
        roleMappingService.deleteById(MongoUtils.toObjectId(id));
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
        long count = roleMappingService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
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
        long count = roleMappingService.count(where, getLoginUser());
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
    public ResponseMessage<RoleMappingDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(roleMappingService.findOne(filter, getLoginUser()));
    }

    /**
     * Update instances of the model matched by {{where}} from the data source.
     *
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody String reqBody) {
        Where where = parseWhere(whereJson);
        Document update = Document.parse(reqBody);

        if (!update.containsKey("$set") && !update.containsKey("$setOnInsert") && !update.containsKey("$unset")) {
            Document _body = new Document();
            _body.put("$set", update);
            update = _body;
        }

        long count = roleMappingService.updateByWhere(where, update, getLoginUser());
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
    public ResponseMessage<RoleMappingDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody RoleMappingDto roleDto) {
        Where where = parseWhere(whereJson);
        return success(roleMappingService.upsertByWhere(where, roleDto, getLoginUser()));
    }
}
