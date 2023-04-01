/**
 * @title: PermissionController
 * @description:
 * @author lk
 * @date 2021/12/13
 */
package com.tapdata.tm.Permission.controller;

import com.tapdata.tm.Permission.entity.PermissionEntity;
import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.utils.WebUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

@Tag(name = "Permission", description = "Permission api")
@RestController
@RequestMapping(value = "/api/Permissions")
public class PermissionController extends BaseController {

	@Autowired
	private PermissionService permissionService;

	@Operation(summary = "Find all permissions of the model matched by filter from the data source")
	@GetMapping
	public ResponseMessage<List<PermissionEntity>> find(
					@Parameter(in = ParameterIn.QUERY, description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`).")
					@RequestParam(value = "filter", required = false) String filterJson, HttpServletRequest request) {
		Filter filter = parseFilter(filterJson);
		List<PermissionEntity> entities = permissionService.find(filter);
		if (CollectionUtils.isNotEmpty(entities)) {
			Locale locale = WebUtils.getLocale(request);
			entities.forEach(e -> {
				String message = MessageUtil.getMessage(locale, e.getName());
				if (!StringUtils.equals(message, e.getName())) {
					e.setDescription(message);
				}
			});

		}
		return success(entities);
	}

	@Operation(summary = "Count instances of the model matched by where from the data source")
	@GetMapping("/count")
	public ResponseMessage<Object> count(
			@Parameter(in = ParameterIn.QUERY, description = "Criteria to match model instances")
			@RequestParam(value = "where", required = false) String whereJson) {
		Filter filter = parseFilter(whereJson);
		long count = permissionService.count(filter.getWhere());
		return success(new HashMap<String, Long>(){{
			put("count", count);
		}});
	}
}
