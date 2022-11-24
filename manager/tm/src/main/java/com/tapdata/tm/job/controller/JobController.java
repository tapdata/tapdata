package com.tapdata.tm.job.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.job.dto.JobDto;
import com.tapdata.tm.job.service.JobService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;


/**
 * @Date: 2021/09/13
 * @Description:
 */
@Tag(name = "Jobs", description = "Jobs相关接口")
@RestController
@RequestMapping("/api/Jobs")
public class JobController extends BaseController {

    @Autowired
    private JobService jobService;

    /**
     * Create a new instance of the model and persist it into the data source
     * @param jobs
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<JobDto> save(@RequestBody JobDto jobs) {
        jobs.setId(null);
        return success(jobService.save(jobs, getLoginUser()));
    }

    /**
     *  Patch an existing model instance or insert a new one into the data source
     * @param jobs
     * @return
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<JobDto> update(@RequestBody JobDto jobs) {
        return success(jobService.save(jobs, getLoginUser()));
    }


    /**
     * Find all instances of the model matched by filter from the data source
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<JobDto>> find(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(jobService.find(filter, getLoginUser()));
    }

    /**
     *  Replace an existing model instance or insert a new one into the data source
     * @param jobs
     * @return
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<JobDto> put(@RequestBody JobDto jobs) {
        return success(jobService.replaceOrInsert(jobs, getLoginUser()));
    }


    /**
     * Check whether a model instance exists in the data source
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        long count = jobService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     *  Patch attributes for a model instance and persist it into the data source
     * @param jobs
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<JobDto> updateById(@PathVariable("id") String id, @RequestBody JobDto jobs) {
        jobs.setId(MongoUtils.toObjectId(id));
        return success(jobService.save(jobs, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<JobDto> findById(@PathVariable("id") String id,
                                            @RequestParam("fields") String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(jobService.findById(MongoUtils.toObjectId(id),  fields, getLoginUser()));
    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param jobs
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PutMapping("{id}")
    public ResponseMessage<JobDto> replceById(@PathVariable("id") String id, @RequestBody JobDto jobs) {
        return success(jobService.replaceById(MongoUtils.toObjectId(id), jobs, getLoginUser()));
    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param jobs
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PostMapping("{id}/replace")
    public ResponseMessage<JobDto> replaceById2(@PathVariable("id") String id, @RequestBody JobDto jobs) {
        return success(jobService.replaceById(MongoUtils.toObjectId(id), jobs, getLoginUser()));
    }



    /**
     * Delete a model instance by {{id}} from the data source
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        jobService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
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
        long count = jobService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
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
        long count = jobService.count(where, getLoginUser());
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
    public ResponseMessage<JobDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(jobService.findOne(filter, getLoginUser()));
    }

    /**
     *  Update instances of the model matched by {{where}} from the data source.
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody String reqBody) {
        Where where = parseWhere(whereJson);
        Document body = Document.parse(reqBody);
        if (!body.containsKey("$set") && !body.containsKey("$setOnInsert") && !body.containsKey("$unset")) {
            Document _body = new Document();
            _body.put("$set", body);
            body = _body;
        }
        long count = jobService.updateByWhere(where, body, getLoginUser());

        /*if (reqBody.indexOf("\"$set\"") > 0) {
            UpdateDto<JobDto> updateDto = JsonUtil.parseJsonUseJackson(reqBody, new TypeReference<UpdateDto<JobDto>>(){});
            count = jobService.updateByWhere(where, updateDto, getLoginUser());
        } else {
            JobDto jobDto = JsonUtil.parseJsonUseJackson(reqBody, JobDto.class);
            count = jobService.updateByWhere(where, jobDto, getLoginUser());
        }*/
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
    public ResponseMessage<JobDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody JobDto jobs) {
        Where where = parseWhere(whereJson);
        return success(jobService.upsertByWhere(where, jobs, getLoginUser()));
    }

}
