/**
 * @title: RoleController
 * @description:
 * @author lk
 * @date 2021/12/1
 */
package com.tapdata.tm.role.controller;

import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import com.tapdata.tm.role.dto.RoleDto;
import com.tapdata.tm.role.service.RoleService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/roles")
public class RoleController extends BaseController {

	private RoleService roleService;
	@Autowired
	private PermissionService permissionService;
	@Autowired
	private SettingsService settingsService;
	public RoleController(RoleService roleService) {
		this.roleService = roleService;
	}

	/**
	 * Create a new instance of the model and persist it into the data source
	 * @param roleDto
	 * @return
	 */
	@Operation(summary = "Create a new instance of the model and persist it into the data source")
	@PostMapping
	public ResponseMessage<RoleDto> save(@RequestBody RoleDto roleDto) {
		if (!settingsService.isCloud() && permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_ROLE_MANAGEMENT, getLoginUser().getUserId())) {
			if (roleDto == null) {
				throw new BizException("IllegalArgument", "roleDto");
			}
			roleDto.setId(null);
			RoleDto dto = roleService.findOne(Query.query(Criteria.where("name").is(roleDto.getName())));
			if (dto != null) {
				throw new BizException("Role.Already.Exists");
			}
			return success(roleService.save(roleDto, getLoginUser()));
		} else {
			throw new BizException("NotAuthorized");
		}
	}

	/**
	 *  Patch an existing model instance or insert a new one into the data source
	 * @param roleDto
	 * @return
	 */
	@Operation(summary = "Patch an existing model instance or insert a new one into the data source")
	@PatchMapping()
	public ResponseMessage<RoleDto> update(@RequestBody RoleDto roleDto) {
		if (!settingsService.isCloud() && permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_ROLE_MANAGEMENT, getLoginUser().getUserId())) {
			return success(roleService.save(roleDto, getLoginUser()));
		} else {
			throw new BizException("NotAuthorized");
		}
	}


	/**
	 * Find all instances of the model matched by filter from the data source
	 * @param filterJson
	 * @return
	 */
	@Operation(summary = "Find all instances of the model matched by filter from the data source")
	@GetMapping
	public ResponseMessage<Page<RoleDto>> find(
			@Parameter(in = ParameterIn.QUERY,
					description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
			)
			@RequestParam(value = "filter", required = false) String filterJson) {
		Filter filter = parseFilter(filterJson);
		if (filter == null) {
			filter = new Filter();
		}
		return success(roleService.find(filter, getLoginUser()));
	}

	/**
	 *  Replace an existing model instance or insert a new one into the data source
	 * @param roleDto
	 * @return
	 */
	@Operation(summary = "Replace an existing model instance or insert a new one into the data source")
	@PutMapping
	public ResponseMessage<RoleDto> put(@RequestBody RoleDto roleDto) {
		if (!settingsService.isCloud() && permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_ROLE_MANAGEMENT, getLoginUser().getUserId())) {
			return success(roleService.replaceOrInsert(roleDto, getLoginUser()));
		} else {
			throw new BizException("NotAuthorized");
		}
	}


	/**
	 * Check whether a model instance exists in the data source
	 * @return
	 */
	@Operation(summary = "Check whether a model instance exists in the data source")
	@RequestMapping(value = "{id}", method = RequestMethod.HEAD)
	public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
		long count = roleService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
		HashMap<String, Boolean> existsValue = new HashMap<>();
		existsValue.put("exists", count > 0);
		return success(existsValue);
	}

	/**
	 *  Patch attributes for a model instance and persist it into the data source
	 * @param roleDto
	 * @return
	 */
	@Operation(summary = "Patch attributes for a model instance and persist it into the data source")
	@PatchMapping("{id}")
	public ResponseMessage<RoleDto> updateById(@PathVariable("id") String id, @RequestBody RoleDto roleDto) {
		if (!settingsService.isCloud() && permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_ROLE_MANAGEMENT, getLoginUser().getUserId())) {
			roleDto.setId(MongoUtils.toObjectId(id));
			return success(roleService.save(roleDto, getLoginUser()));
		} else {
			throw new BizException("NotAuthorized");
		}

	}


	/**
	 * Find a model instance by {{id}} from the data source
	 * @param fieldsJson
	 * @return
	 */
	@Operation(summary = "Find a model instance by {{id}} from the data source")
	@GetMapping("{id}")
	public ResponseMessage<RoleDto> findById(@PathVariable("id") String id,
	                                           @RequestParam("fields") String fieldsJson) {
		Field fields = parseField(fieldsJson);
		return success(roleService.findById(MongoUtils.toObjectId(id),  fields, getLoginUser()));
	}

	/**
	 *  Replace attributes for a model instance and persist it into the data source.
	 * @param roleDto
	 * @return
	 */
	@Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
	@PutMapping("{id}")
	public ResponseMessage<RoleDto> replceById(@PathVariable("id") String id, @RequestBody RoleDto roleDto) {
		return success(roleService.replaceById(MongoUtils.toObjectId(id), roleDto, getLoginUser()));
	}

	/**
	 *  Replace attributes for a model instance and persist it into the data source.
	 * @param roleDto
	 * @return
	 */
	@Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
	@PostMapping("{id}/replace")
	public ResponseMessage<RoleDto> replaceById2(@PathVariable("id") String id, @RequestBody RoleDto roleDto) {
		return success(roleService.replaceById(MongoUtils.toObjectId(id), roleDto, getLoginUser()));
	}



	/**
	 * Delete a model instance by {{id}} from the data source
	 * @param id
	 * @return
	 */
	@Operation(summary = "Delete a model instance by {{id}} from the data source")
	@DeleteMapping("{id}")
	public ResponseMessage<Void> delete(@PathVariable("id") String id) {
		DataPermissionHelper.cleanAuthOfRoleDelete(Collections.singleton(id));
		roleService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
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
		long count = roleService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
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
		long count = roleService.count(where, getLoginUser());
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
	public ResponseMessage<RoleDto> findOne(
			@Parameter(in = ParameterIn.QUERY,
					description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
			)
			@RequestParam(value = "filter", required = false) String filterJson) {
		Filter filter = parseFilter(filterJson);
		if (filter == null) {
			filter = new Filter();
		}
		return success(roleService.findOne(filter, getLoginUser()));
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
		Document update = Document.parse(reqBody);

		if (!update.containsKey("$set") && !update.containsKey("$setOnInsert") && !update.containsKey("$unset")) {
			Document _body = new Document();
			_body.put("$set", update);
			update = _body;
		}

		long count = roleService.updateByWhere(where, update, getLoginUser());
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
	public ResponseMessage<RoleDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody RoleDto roleDto) {
		if (!settingsService.isCloud() && permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_ROLE_MANAGEMENT, getLoginUser().getUserId())) {
			Where where = parseWhere(whereJson);
			return success(roleService.upsertByWhere(where, roleDto, getLoginUser()));
		} else {
			throw new BizException("NotAuthorized");
		}

	}
}
