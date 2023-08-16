package com.tapdata.tm.permissions.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.base.DataPermissionAction;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionTypeEnums;
import com.tapdata.tm.permissions.service.DataPermissionService;
import com.tapdata.tm.permissions.vo.DataPermissionAuthVo;
import com.tapdata.tm.permissions.vo.DataPermissionSaveVo;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/8 18:13 Create
 */
@Tag(name = "DataPermission", description = "Data Permission API")
@RestController
@RequestMapping(value = "/api/data-permission")
public class DataPermissionController extends BaseController {

	@Autowired
	private DataPermissionService service;

	@GetMapping("/data-actions")
	public ResponseMessage<Set<String>> findDataActions(
		@RequestParam(name = "dataType") String dataType,
		@RequestParam(name = "dataId") String dataId
	) {
		DataPermissionDataTypeEnums dataPermissionDataTypeEnums = DataPermissionDataTypeEnums.parse(dataType);
		if (null == dataPermissionDataTypeEnums) throw new BizException("IllegalArgument", "dataType");

		UserDetail userDetail = getLoginUser();
		return success(service.findDataActions(userDetail, dataPermissionDataTypeEnums, new ObjectId(dataId)));
	}

	@GetMapping("/permissions")
	public ResponseMessage<List<DataPermissionAction>> findDataPermissions(
		@RequestParam(name = "dataType") String dataType,
		@RequestParam(name = "dataId") String dataId
	) {
		DataPermissionDataTypeEnums dataPermissionDataTypeEnums = DataPermissionDataTypeEnums.parse(dataType);
		if (null == dataPermissionDataTypeEnums) throw new BizException("IllegalArgument", "dataType");

		return success(service.findDataPermissions(dataPermissionDataTypeEnums, new ObjectId(dataId)));
	}

	@PostMapping("/permissions")
	public ResponseMessage<Long> postDataPermissions(@RequestBody DataPermissionSaveVo vo) {
		DataPermissionDataTypeEnums dataPermissionDataTypeEnums = DataPermissionDataTypeEnums.parse(vo.getDataType());
		if (null == dataPermissionDataTypeEnums) throw new BizException("IllegalArgument", "dataType");
		if (null == vo.getDataId()) throw new BizException("IllegalArgument", "dataId");

		List<DataPermissionAction> actions = vo.getActions();
		if (null != actions) {
			Map<String, DataPermissionAction> map = new LinkedHashMap<>();
			for (DataPermissionAction a : vo.getActions()) {
				if (null == a.getActions() || a.getActions().isEmpty()) continue;
				map.put(String.format("%s,%s", a.getType(), a.getTypeId()), a);
			}

			actions = new ArrayList<>(map.values());
		}


		UserDetail userDetail = getLoginUser();
		long modifyCounts = service.saveDataPermissions(userDetail, dataPermissionDataTypeEnums, new ObjectId(vo.getDataId()), actions);
		return success(modifyCounts);
	}

	@GetMapping("/role-actions")
	public ResponseMessage<Set<String>> findRoleDataActions(
		@RequestParam(name = "typeId") String typeId,
		@RequestParam(name = "dataType") String dataType,
		@RequestParam(name = "dataIds") String dataIds
	) {
		DataPermissionDataTypeEnums dataPermissionDataTypeEnums = DataPermissionDataTypeEnums.parse(dataType);
		if (null == dataPermissionDataTypeEnums) throw new BizException("IllegalArgument", "dataType");
		if (null == dataIds || dataIds.isEmpty()) throw new BizException("IllegalArgument", "dataIds");
		if (StringUtils.isBlank(typeId)) throw new BizException("IllegalArgument", "typeId");

		Set<ObjectId> objectIds = new HashSet<>();
		for (String s : dataIds.split(",")) {
			s = s.trim();
			if (s.isEmpty()) continue;
			objectIds.add(new ObjectId(s));
		}

		return success(service.findDataActionsOfType(DataPermissionTypeEnums.Role, typeId, dataPermissionDataTypeEnums, objectIds));
	}

	@PostMapping("/data-auth")
	public ResponseMessage<Object> dataAuth(@RequestBody DataPermissionAuthVo vo) {
		DataPermissionDataTypeEnums dataPermissionDataType = DataPermissionDataTypeEnums.parse(vo.getDataType());
		if (null == dataPermissionDataType) throw new BizException("IllegalArgument", "dataType");
		if (null == vo.getDataIds() || vo.getDataIds().isEmpty()) throw new BizException("IllegalArgument", "dataIds");
		DataPermissionTypeEnums dataPermissionType = DataPermissionTypeEnums.parse(vo.getType());
		if (null == dataPermissionType) throw new BizException("IllegalArgument", "type");
		if (null == vo.getTypeIds() || vo.getTypeIds().isEmpty()) throw new BizException("IllegalArgument", "typeIds");

		Set<ObjectId> objectIds = new HashSet<>();
		Set<String> ignoreIds = new HashSet<>();
		UserDetail userDetail = getLoginUser();
		// admin has all data actions, other user check has data actions
		if (userDetail.isRoot()) {
			for (String dataId : vo.getDataIds()) {
				objectIds.add(new ObjectId(dataId));
			}
		} else {
			for (String dataId : vo.getDataIds()) {
				Set<String> userCreateIds = service.findDataActions(userDetail, dataPermissionDataType, new ObjectId(dataId));
				for (String id : vo.getDataIds()) {
					if (userCreateIds.contains(id)) {
						objectIds.add(new ObjectId(id));
					} else {
						ignoreIds.add(id);
					}
				}
			}
		}
		if (!objectIds.isEmpty()) {
			service.dataAuth(dataPermissionDataType, objectIds, dataPermissionType, vo.getTypeIds(), vo.getActions());
		}

		return success(ignoreIds);
	}

//	@GetMapping("/test")
//	public String test() {
//		UserDetail userDetail = getLoginUser();
//		UserDetail u2 = getLoginUser(userDetail.getUserId());
//		return JSON.toJSONString(u2);
//	}
}
