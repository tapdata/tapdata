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
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/8 15:58 Create
 */
@Service
public class DataPermissionService {

	private static DataPermissionService instance;

	public static Boolean isCloud() {
		return instance.isCloud;
	}

	public static DataPermissionService getInstance() {
		return instance;
	}

	public DataPermissionService() {
		instance = this;
	}

	private Boolean isCloud;
	@Autowired
	private RoleMappingService roleMappingService;
	@Autowired
	private MongoTemplate mongoOperations;

	@Autowired
	public void setProductComponent(ProductComponent productComponent) {
		this.isCloud = productComponent.isCloud();
	}

	public Set<String> getRoleIds(String uid) {
		Set<String> roleIds = new HashSet<>();
		List<RoleMappingDto> mappingDtoList = roleMappingService.getUser(PrincipleType.USER, uid);
		for (RoleMappingDto dto : mappingDtoList) {
			roleIds.add(dto.getRoleId().toHexString());
		}
		return roleIds;
	}

	public boolean isMenuAuth(String menuName, Set<ObjectId> roleIds) {
		Query query = Query.query(Criteria.where("principalId").is(menuName)
			.and("principalType").is(PrincipleType.PERMISSION)
			.and("roleId").in(roleIds));
		return roleMappingService.count(query) > 0;
	}

	public Set<String> filterNotCreator(UserDetail userDetail, DataPermissionDataTypeEnums dataType, Set<ObjectId> dataIds) {
		Query query = Query.query(Criteria.where("_id").in(dataIds).and("user_id").is(userDetail.getUserId()));
		query.fields().include("_id");
		List<BaseEntity> userCreateIds = mongoOperations.find(query, BaseEntity.class, dataType.getCollection());
		return userCreateIds.stream().map(o -> o.getId().toHexString()).collect(Collectors.toSet());
	}

	public Set<String> findDataActions(UserDetail userDetail, DataPermissionDataTypeEnums dataType, ObjectId dataId) {
		if (!userDetail.isRoot()) {
			return findDataActions(userDetail.getUserId(), dataType, dataId, () -> getRoleIds(userDetail.getUserId()));
		}
		return dataType.allActions();
	}

	public Set<String> findDataActions(String userId, DataPermissionDataTypeEnums dataType, ObjectId dataId, Supplier<Set<String>> roleSetSupplier) {
		Query query = Query.query(Criteria.where("_id").is(dataId));
		query.fields().include("user_id", DataPermissionHelper.FIELD_NAME);
		BaseEntity record = mongoOperations.findOne(query, BaseEntity.class, dataType.getCollection());
		if (null == record) return null;

		if (!userId.equals(record.getUserId())) {
			Set<String> roleIds = roleSetSupplier.get();
			return DataPermissionHelper.mergeActions(new HashSet<>(), roleIds, record.getPermissions());
		}

		return dataType.allActions();
	}

	public List<DataPermissionAction> findDataPermissions(DataPermissionDataTypeEnums dataType, ObjectId dataId) {
		Query query = Query.query(Criteria.where("_id").is(dataId));
		query.fields().include("user_id", DataPermissionHelper.FIELD_NAME);
		BaseEntity record = mongoOperations.findOne(query, BaseEntity.class, dataType.getCollection());
		if (null == record) return null;

		return record.getPermissions();
	}

	public DataPermissionAuthInfoVo setDataActions(DataPermissionAuthInfoVo authInfoVo, DataPermissionTypeVo... inTypes) {
		Criteria whereCriteria = Criteria.where(DataPermissionHelper.AUTH_ACTIONS).is(authInfoVo.getActionEnums().name());

		List<Criteria> orList = new ArrayList<>();
		orList.add(Criteria.where("user_id").is(authInfoVo.getUserId()));
		if (null != inTypes) {
			for (DataPermissionTypeVo typeVo : inTypes) {
				orList.add(Criteria
					.where(DataPermissionHelper.AUTH_TYPE).is(typeVo.getType().name())
					.and(DataPermissionHelper.AUTH_TYPE_ID).in(typeVo.getIds())
				);
			}
		}
		whereCriteria.orOperator(orList);

		AggregationResults<BaseDto> results = mongoOperations.aggregate(Aggregation.newAggregation(
			Aggregation.match(Criteria.where("_id").is(MongoUtils.toObjectId(authInfoVo.getId())))
			, Aggregation.project(DataPermissionHelper.FIELD_NAME, "user_id")
			, Aggregation.unwind(DataPermissionHelper.FIELD_NAME)
			, Aggregation.match(whereCriteria)
			, Aggregation.unwind(DataPermissionHelper.AUTH_ACTIONS, "user_id")
			, Aggregation.group()
				.addToSet(DataPermissionHelper.AUTH_ACTIONS).as("permissionActions")
				.first("user_id").as("user_id")
		), authInfoVo.getDataTypeEnums().getCollection(), BaseDto.class);

		BaseDto dto = results.getUniqueMappedResult();
		if (null != dto) {
			if (authInfoVo.getUserId().equals(dto.getUserId())) {
				return authInfoVo.authWithUserData().actionSet(authInfoVo.getDataTypeEnums().allActions());
			} else if (null != dto.getPermissionActions() && !dto.getPermissionActions().isEmpty()) {
				return authInfoVo.authWithRole().actionSet(dto.getPermissionActions());
			}
		}
		return authInfoVo;
	}

	public long saveDataPermissions(UserDetail userDetail, DataPermissionDataTypeEnums dataType, ObjectId dataId, List<DataPermissionAction> actions) {
		Criteria criteria = Criteria.where("_id").is(dataId);
		if (!userDetail.isRoot()) {
			criteria.and("user_id").is(userDetail.getUserId());
		}
		UpdateResult updateResult = mongoOperations.updateFirst(Query.query(criteria), Update.update(DataPermissionHelper.FIELD_NAME, actions), dataType.getCollection());
		return updateResult.getModifiedCount();
	}

	public Set<String> findDataActionsOfType(DataPermissionTypeEnums type, String typeId, DataPermissionDataTypeEnums dataType, Set<ObjectId> dataIds) {
		Query query = Query.query(Criteria.where("_id").in(dataIds));
		query.fields().include(DataPermissionHelper.FIELD_NAME);
		List<BaseEntity> records = mongoOperations.find(query, BaseEntity.class, dataType.getCollection());

		List<DataPermissionAction> permissions;
		Set<String> typeIds = Collections.singleton(typeId);
		Set<String> results = new HashSet<>();
		for (BaseEntity record : records) {
			permissions = record.getPermissions();
			if (null == permissions) continue;
			permissions = permissions.stream().filter(p -> type.name().equals(p.getType()) && typeId.equals(p.getTypeId())).collect(Collectors.toList());
			DataPermissionHelper.mergeActions(results, typeIds, permissions);
		}
		return results;
	}

	public void cleanAuthOfRoleDelete(Set<String> roleIds) {
		if (isCloud) return;
		for (DataPermissionDataTypeEnums dataType : DataPermissionDataTypeEnums.values()) {
			Query query = Query.query(Criteria.where(DataPermissionHelper.FIELD_NAME).exists(true));
			Update update = Update.fromDocument(new Document("$pull", new Document(DataPermissionHelper.FIELD_NAME
				, new Document("type", DataPermissionTypeEnums.Role).append("typeId", new Document("$in", roleIds))
			)));
			mongoOperations.updateMulti(query, update, dataType.getCollection());
		}
	}

	public void dataAuth(DataPermissionDataTypeEnums dataType, Set<ObjectId> dataIds, DataPermissionTypeEnums type, Set<String> typeIds, Set<String> actions) {
		// clean auth of role first
		Query query = Query.query(Criteria.where("_id").in(dataIds));
		Update update = Update.fromDocument(new Document("$pull", new Document(DataPermissionHelper.FIELD_NAME
			, new Document("type", type).append("typeId", new Document("$in", typeIds))
		)));
		mongoOperations.updateMulti(query, update, dataType.getCollection());

		// actions is null or empty do not anything
		if (null != actions && !actions.isEmpty()) {
			for (String tid : typeIds) {
				 update = Update.fromDocument(new Document("$addToSet", new Document(DataPermissionHelper.FIELD_NAME,new DataPermissionAction(type.name(), tid, actions))));
				//update = Update.update(DataPermissionHelper.FIELD_NAME, actionList);
				mongoOperations.updateMulti(query, update, dataType.getCollection());
			}
		}
	}
}
