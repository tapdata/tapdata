package com.tapdata.tm.sdkVersion.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.sdkVersion.dto.SdkVersionDto;
import com.tapdata.tm.sdkVersion.service.SdkVersionService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;


/**
 * @Date: 2025/07/02
 * @Description:
 */
@Tag(name = "SdkVersion", description = "sdkVersion相关接口")
@RestController
@RequestMapping("/api/SdkVersion")
public class SdkVersionController extends BaseController {

	private final SdkVersionService sdkVersionService;

	public SdkVersionController(SdkVersionService sdkVersionService) {
		this.sdkVersionService = sdkVersionService;
	}

	/**
	 * Create a new instance of the model and persist it into the data source
	 *
	 * @param sdkVersion
	 * @return
	 */
	@Operation(summary = "Create a new instance of the model and persist it into the data source")
	@PostMapping
	public ResponseMessage<SdkVersionDto> save(@RequestBody SdkVersionDto sdkVersion) {
		sdkVersion.setId(null);
		return success(sdkVersionService.save(sdkVersion, getLoginUser()));
	}

	/**
	 * Patch an existing model instance or insert a new one into the data source
	 *
	 * @param sdkVersion
	 * @return
	 */
	@Operation(summary = "Patch an existing model instance or insert a new one into the data source")
	@PatchMapping()
	public ResponseMessage<SdkVersionDto> update(@RequestBody SdkVersionDto sdkVersion) {
		return success(sdkVersionService.save(sdkVersion, getLoginUser()));
	}


	/**
	 * Find all instances of the model matched by filter from the data source
	 *
	 * @param filterJson
	 * @return
	 */
	@Operation(summary = "Find all instances of the model matched by filter from the data source")
	@GetMapping
	public ResponseMessage<Page<SdkVersionDto>> find(
			@Parameter(in = ParameterIn.QUERY,
					description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
			)
			@RequestParam(value = "filter", required = false) String filterJson) {
		Filter filter = parseFilter(filterJson);
		if (filter == null) {
			filter = new Filter();
		}
		return success(sdkVersionService.find(filter, getLoginUser()));
	}

	/**
	 * Replace an existing model instance or insert a new one into the data source
	 *
	 * @param sdkVersion
	 * @return
	 */
	@Operation(summary = "Replace an existing model instance or insert a new one into the data source")
	@PutMapping
	public ResponseMessage<SdkVersionDto> put(@RequestBody SdkVersionDto sdkVersion) {
		return success(sdkVersionService.replaceOrInsert(sdkVersion, getLoginUser()));
	}


	/**
	 * Check whether a model instance exists in the data source
	 *
	 * @return
	 */
	@Operation(summary = "Check whether a model instance exists in the data source")
	@RequestMapping(value = "{id}", method = RequestMethod.HEAD)
	public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
		long count = sdkVersionService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
		HashMap<String, Boolean> existsValue = new HashMap<>();
		existsValue.put("exists", count > 0);
		return success(existsValue);
	}

	/**
	 * Patch attributes for a model instance and persist it into the data source
	 *
	 * @param sdkVersion
	 * @return
	 */
	@Operation(summary = "Patch attributes for a model instance and persist it into the data source")
	@PatchMapping("{id}")
	public ResponseMessage<SdkVersionDto> updateById(@PathVariable("id") String id, @RequestBody SdkVersionDto sdkVersion) {
		sdkVersion.setId(MongoUtils.toObjectId(id));
		return success(sdkVersionService.save(sdkVersion, getLoginUser()));
	}


	/**
	 * Find a model instance by {{id}} from the data source
	 *
	 * @param fieldsJson
	 * @return
	 */
	@Operation(summary = "Find a model instance by {{id}} from the data source")
	@GetMapping("{id}")
	public ResponseMessage<SdkVersionDto> findById(@PathVariable("id") String id,
												   @RequestParam(value = "fields", required = false) String fieldsJson) {
		Field fields = parseField(fieldsJson);
		return success(sdkVersionService.findById(MongoUtils.toObjectId(id), fields, getLoginUser()));
	}

	/**
	 * Replace attributes for a model instance and persist it into the data source.
	 *
	 * @param sdkVersion
	 * @return
	 */
	@Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
	@PutMapping("{id}")
	public ResponseMessage<SdkVersionDto> replceById(@PathVariable("id") String id, @RequestBody SdkVersionDto sdkVersion) {
		return success(sdkVersionService.replaceById(MongoUtils.toObjectId(id), sdkVersion, getLoginUser()));
	}

	/**
	 * Replace attributes for a model instance and persist it into the data source.
	 *
	 * @param sdkVersion
	 * @return
	 */
	@Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
	@PostMapping("{id}/replace")
	public ResponseMessage<SdkVersionDto> replaceById2(@PathVariable("id") String id, @RequestBody SdkVersionDto sdkVersion) {
		return success(sdkVersionService.replaceById(MongoUtils.toObjectId(id), sdkVersion, getLoginUser()));
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
		sdkVersionService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
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
		long count = sdkVersionService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
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
		long count = sdkVersionService.count(where, getLoginUser());
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
	public ResponseMessage<SdkVersionDto> findOne(
			@Parameter(in = ParameterIn.QUERY,
					description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
			)
			@RequestParam(value = "filter", required = false) String filterJson) {
		Filter filter = parseFilter(filterJson);
		if (filter == null) {
			filter = new Filter();
		}
		return success(sdkVersionService.findOne(filter, getLoginUser()));
	}

	/**
	 * Update instances of the model matched by {{where}} from the data source.
	 *
	 * @param whereJson
	 * @return
	 */
	@Operation(summary = "Update instances of the model matched by {{where}} from the data source")
	@PostMapping("update")
	public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody SdkVersionDto sdkVersion) {
		Where where = parseWhere(whereJson);
		long count = sdkVersionService.updateByWhere(where, sdkVersion, getLoginUser());
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
	public ResponseMessage<SdkVersionDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody SdkVersionDto sdkVersion) {
		Where where = parseWhere(whereJson);
		return success(sdkVersionService.upsertByWhere(where, sdkVersion, getLoginUser()));
	}

}