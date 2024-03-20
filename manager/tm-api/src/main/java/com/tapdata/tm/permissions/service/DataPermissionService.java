package com.tapdata.tm.permissions.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.base.DataPermissionAction;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.config.component.ProductComponent;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionTypeEnums;
import com.tapdata.tm.permissions.vo.DataPermissionAuthInfoVo;
import com.tapdata.tm.permissions.vo.DataPermissionTypeVo;
import com.tapdata.tm.roleMapping.dto.PrincipleType;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import com.tapdata.tm.utils.MongoUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/8 15:58 Create
 */
public abstract class DataPermissionService {
	static DataPermissionService instance;

	public static Boolean isCloud() {
		return instance.isCloud;
	}

	public static DataPermissionService getInstance() {
		return instance;
	}

	public DataPermissionService() {
		instance = this;
	}
	@Autowired
	public void setProductComponent(ProductComponent productComponent) {
		this.isCloud = productComponent.isCloud();
	}
	public Boolean isCloud;

	public abstract Set<String> getRoleIds(String uid);

	public abstract boolean isMenuAuth(String menuName, Set<ObjectId> roleIds);

	public abstract Set<String> filterNotCreator(UserDetail userDetail, DataPermissionDataTypeEnums dataType, Set<ObjectId> dataIds);

	public abstract Set<String> findDataActions(UserDetail userDetail, DataPermissionDataTypeEnums dataType, ObjectId dataId);

	public abstract Set<String> findDataActions(String userId, DataPermissionDataTypeEnums dataType, ObjectId dataId, Supplier<Set<String>> roleSetSupplier);

	public abstract List<DataPermissionAction> findDataPermissions(DataPermissionDataTypeEnums dataType, ObjectId dataId);

	public abstract DataPermissionAuthInfoVo setDataActions(DataPermissionAuthInfoVo authInfoVo, DataPermissionTypeVo... inTypes);

	public abstract long saveDataPermissions(UserDetail userDetail, DataPermissionDataTypeEnums dataType, ObjectId dataId, List<DataPermissionAction> actions);

	public abstract Set<String> findDataActionsOfType(DataPermissionTypeEnums type, String typeId, DataPermissionDataTypeEnums dataType, Set<ObjectId> dataIds);

	public abstract void cleanAuthOfRoleDelete(Set<String> roleIds);

	public abstract void dataAuth(DataPermissionDataTypeEnums dataType, Set<ObjectId> dataIds, DataPermissionTypeEnums type, Set<String> typeIds, Set<String> actions);
}
