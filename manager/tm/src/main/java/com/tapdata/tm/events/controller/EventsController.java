/**
 * @title: EventsController
 * @description:
 * @author lk
 * @date 2021/12/28
 */
package com.tapdata.tm.events.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.events.dto.EventsDto;
import com.tapdata.tm.events.service.EventsService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@Tag(name = "Events", description = "Events api")
@RestController
@Slf4j
@RequestMapping("/api/Events")
public class EventsController extends BaseController {
	
	@Autowired
	private EventsService eventsService;
	
	/**
	 * Create a new instance of the model and persist it into the data source
	 * @param eventsDto eventsDto
	 * @return ResponseMessage
	 */
	@Operation(summary = "Create a new instance of the model and persist it into the data source")
	@PostMapping
	public ResponseMessage<EventsDto> save(@RequestBody EventsDto eventsDto) {
		eventsDto.setId(null);
		return success(eventsService.save(eventsDto, getLoginUser()));
	}

	/**
	 *  Patch an existing model instance or insert a new one into the data source
	 * @param eventsDto eventsDto
	 * @return ResponseMessage
	 */
	@Operation(summary = "Patch an existing model instance or insert a new one into the data source")
	@PatchMapping()
	public ResponseMessage<EventsDto> update(@RequestBody EventsDto eventsDto) {
		return success(eventsService.save(eventsDto, getLoginUser()));
	}


	/**
	 * Find all instances of the model matched by filter from the data source
	 * @param filterJson  filter
	 * @return ResponseMessage
	 */
	@Operation(summary = "Find all instances of the model matched by filter from the data source")
	@GetMapping
	public ResponseMessage<Page<EventsDto>> find(
			@Parameter(in = ParameterIn.QUERY,
					description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
			)
			@RequestParam(value = "filter", required = false) String filterJson) {
		Filter filter = parseFilter(filterJson);
		if (filter == null) {
			filter = new Filter();
		}
		return success(eventsService.find(filter, getLoginUser()));
	}

	/**
	 *  Replace an existing model instance or insert a new one into the data source
	 * @param eventsDto eventsDto
	 * @return ResponseMessage
	 */
	@Operation(summary = "Replace an existing model instance or insert a new one into the data source")
	@PutMapping
	public ResponseMessage<EventsDto> put(@RequestBody EventsDto eventsDto) {
		return success(eventsService.replaceOrInsert(eventsDto, getLoginUser()));
	}


	/**
	 * Check whether a model instance exists in the data source
	 * @return ResponseMessage
	 */
	@Operation(summary = "Check whether a model instance exists in the data source")
	@RequestMapping(value = "{id}", method = RequestMethod.HEAD)
	public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
		long count = eventsService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
		HashMap<String, Boolean> existsValue = new HashMap<>();
		existsValue.put("exists", count > 0);
		return success(existsValue);
	}

	/**
	 *  Patch attributes for a model instance and persist it into the data source
	 * @param eventsDto eventsDto
	 * @return ResponseMessage
	 */
	@Operation(summary = "Patch attributes for a model instance and persist it into the data source")
	@PatchMapping("{id}")
	public ResponseMessage<EventsDto> updateById(@PathVariable("id") String id, @RequestBody EventsDto eventsDto) {
		eventsDto.setId(MongoUtils.toObjectId(id));
		return success(eventsService.save(eventsDto, getLoginUser()));
	}


	/**
	 * Find a model instance by {{id}} from the data source
	 * @param fieldsJson fieldsJson
	 * @return ResponseMessage
	 */
	@Operation(summary = "Find a model instance by {{id}} from the data source")
	@GetMapping("{id}")
	public ResponseMessage<EventsDto> findById(@PathVariable("id") String id,
	                                                  @RequestParam(value = "fields", required = false) String fieldsJson) {
		Field fields = parseField(fieldsJson);
		return success(eventsService.findById(MongoUtils.toObjectId(id),  fields, getLoginUser()));
	}

	/**
	 *  Replace attributes for a model instance and persist it into the data source.
	 * @param eventsDto eventsDto
	 * @return ResponseMessage
	 */
	@Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
	@PutMapping("{id}")
	public ResponseMessage<EventsDto> replceById(@PathVariable("id") String id, @RequestBody EventsDto eventsDto) {
		return success(eventsService.replaceById(MongoUtils.toObjectId(id), eventsDto, getLoginUser()));
	}

	/**
	 *  Replace attributes for a model instance and persist it into the data source.
	 * @param eventsDto eventsDto
	 * @return ResponseMessage
	 */
	@Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
	@PostMapping("{id}/replace")
	public ResponseMessage<EventsDto> replaceById2(@PathVariable("id") String id, @RequestBody EventsDto eventsDto) {
		return success(eventsService.replaceById(MongoUtils.toObjectId(id), eventsDto, getLoginUser()));
	}



	/**
	 * Delete a model instance by {{id}} from the data source
	 * @param id id
	 * @return ResponseMessage
	 */
	@Operation(summary = "Delete a model instance by {{id}} from the data source")
	@DeleteMapping("{id}")
	public ResponseMessage<Void> delete(@PathVariable("id") String id) {
		eventsService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
		return success();
	}

	/**
	 *  Check whether a model instance exists in the data source
	 * @param id id
	 * @return ResponseMessage
	 */
	@Operation(summary = "Check whether a model instance exists in the data source")
	@GetMapping("{id}/exists")
	public ResponseMessage<HashMap<String, Boolean>> checkById1(@PathVariable("id") String id) {
		long count = eventsService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
		HashMap<String, Boolean> existsValue = new HashMap<>();
		existsValue.put("exists", count > 0);
		return success(existsValue);
	}

	/**
	 *  Count instances of the model matched by where from the data source
	 * @param whereJson whereJson
	 * @return ResponseMessage
	 */
	@Operation(summary = "Count instances of the model matched by where from the data source")
	@GetMapping("count")
	public ResponseMessage<HashMap<String, Long>> count(@RequestParam("where") String whereJson) {
		Where where = parseWhere(whereJson);
		if (where == null) {
			where = new Where();
		}
		long count = eventsService.count(where, getLoginUser());
		HashMap<String, Long> countValue = new HashMap<>();
		countValue.put("count", count);
		return success(countValue);
	}

	/**
	 *  Find first instance of the model matched by filter from the data source.
	 * @param filterJson filterJson
	 * @return ResponseMessage
	 */
	@Operation(summary = "Find first instance of the model matched by filter from the data source.")
	@GetMapping("findOne")
	public ResponseMessage<EventsDto> findOne(
			@Parameter(in = ParameterIn.QUERY,
					description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
			)
			@RequestParam(value = "filter", required = false) String filterJson) {
		Filter filter = parseFilter(filterJson);
		if (filter == null) {
			filter = new Filter();
		}
		return success(eventsService.findOne(filter, getLoginUser()));
	}

	/**
	 *  Update instances of the model matched by {{where}} from the data source.
	 * @param whereJson whereJson
	 * @return ResponseMessage
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
		Document document = body.get("$set", Document.class);
		long count = eventsService.updateByWhere(where, body, getLoginUser());
		HashMap<String, Long> countValue = new HashMap<>();
		countValue.put("count", count);
		return success(countValue);
	}

	/**
	 *  Update an existing model instance or insert a new one into the data source based on the where criteria.
	 * @param whereJson whereJson
	 * @return ResponseMessage
	 */
	@Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
	@PostMapping("upsertWithWhere")
	public ResponseMessage<EventsDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody EventsDto eventsDto) {
		Where where = parseWhere(whereJson);
		return success(eventsService.upsertByWhere(where, eventsDto, getLoginUser()));
	}

}
