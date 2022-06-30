package com.tapdata.tm.typemappings.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.typemappings.dto.DatabaseTypeResDto;
import com.tapdata.tm.typemappings.dto.TypeMappingsDto;
import com.tapdata.tm.typemappings.service.TypeMappingsService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
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


/**
 * @Date: 2021/09/14
 * @Description:
 */
@Tag(name = "TypeMappings", description = "TypeMappings相关接口")
@RestController
@RequestMapping("/api/TypeMappings")
public class TypeMappingsController extends BaseController {

    @Autowired
    private TypeMappingsService typeMappingsService;

    /**
     * Find all instances of the model matched by filter from the data source
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<TypeMappingsDto>> find(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(typeMappingsService.find(filter, getLoginUser()));
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
        long count = typeMappingsService.count(where, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    @Operation(summary = "Find all instances of the model matched by dataType from the data source")
    @RequestMapping(path = "/dataType", method = {RequestMethod.GET, RequestMethod.POST})
    public ResponseMessage<List<DatabaseTypeResDto>> findByDataType(@RequestParam("databaseType") String databaseType) {
        return success(typeMappingsService.findByDataType(databaseType));
    }

    @Operation(summary = "Find all instances of the model matched by dataType from the data source")
    @RequestMapping(path = "/pdk/dataType", method = {RequestMethod.GET, RequestMethod.POST})
    public ResponseMessage<String> findPdkByDataType(@RequestParam("databaseType") String databaseType) {
        return success(typeMappingsService.findByDataType(databaseType, getLoginUser()));
    }


    @PostMapping("deleteAll")
    public ResponseMessage<Void> deleteAll(@RequestParam("where") String whereJson) {
        Where where = parseWhere(whereJson);
        if (where == null) {
            where = new Where();
        }

        typeMappingsService.deleteAll(where);
        return success();
    }


    /**
     * Create a new instance of the model and persist it into the data source
     *
     * @param typeMappingsDto
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<TypeMappingsDto> save(@RequestBody TypeMappingsDto typeMappingsDto) {
        typeMappingsDto.setId(null);
        return success(typeMappingsService.save(typeMappingsDto, getLoginUser()));
    }

    /**
     * Patch an existing model instance or insert a new one into the data source
     *
     * @param typeMappingsDto
     * @return
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<TypeMappingsDto> update(@RequestBody TypeMappingsDto typeMappingsDto) {
        return success(typeMappingsService.save(typeMappingsDto, getLoginUser()));
    }

    /**
     * Replace an existing model instance or insert a new one into the data source
     *
     * @param typeMappingsDto
     * @return
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<TypeMappingsDto> put(@RequestBody TypeMappingsDto typeMappingsDto) {
        return success(typeMappingsService.replaceOrInsert(typeMappingsDto, getLoginUser()));
    }


    /**
     * Check whether a model instance exists in the data source
     *
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        long count = typeMappingsService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     * Patch attributes for a model instance and persist it into the data source
     *
     * @param typeMappingsDto
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<TypeMappingsDto> updateById(@PathVariable("id") String id, @RequestBody TypeMappingsDto typeMappingsDto) {
        ObjectId objectId = MongoUtils.toObjectId(id);
        typeMappingsDto.setId(objectId);
        return success(typeMappingsService.save(typeMappingsDto, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     *
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<TypeMappingsDto> findById(@PathVariable("id") String id,
                                                     @RequestParam(value = "fields", required = false) String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(typeMappingsService.findById(MongoUtils.toObjectId(id), fields, getLoginUser()));
    }

    /**
     * Replace attributes for a model instance and persist it into the data source.
     *
     * @param typeMappingsDto
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PutMapping("{id}")
    public ResponseMessage<TypeMappingsDto> replceById(@PathVariable("id") String id, @RequestBody TypeMappingsDto typeMappingsDto) {
        return success(typeMappingsService.replaceById(MongoUtils.toObjectId(id), typeMappingsDto, getLoginUser()));
    }

    /**
     * Replace attributes for a model instance and persist it into the data source.
     *
     * @param typeMappingsDto
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PostMapping("{id}/replace")
    public ResponseMessage<TypeMappingsDto> replaceById2(@PathVariable("id") String id, @RequestBody TypeMappingsDto typeMappingsDto) {
        return success(typeMappingsService.replaceById(MongoUtils.toObjectId(id), typeMappingsDto, getLoginUser()));
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
        typeMappingsService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
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
        long count = typeMappingsService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     * Find first instance of the model matched by filter from the data source.
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<TypeMappingsDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(typeMappingsService.findOne(filter, getLoginUser()));
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
        UserDetail user = getLoginUser();
        Document body = Document.parse(reqBody);
        if (!body.containsKey("$set") && !body.containsKey("$setOnInsert") && !body.containsKey("$unset")) {
            Document _body = new Document();
            _body.put("$set", body);
            body = _body;
        }
        long count = typeMappingsService.updateByWhere(where, body, user);
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
    public ResponseMessage<TypeMappingsDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody TypeMappingsDto typeMappingsDto) {
        Where where = parseWhere(whereJson);
        return success(typeMappingsService.upsertByWhere(where, typeMappingsDto, getLoginUser()));
    }
}
