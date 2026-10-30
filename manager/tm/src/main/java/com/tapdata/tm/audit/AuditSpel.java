package com.tapdata.tm.audit;

import com.tapdata.tm.modules.constant.ModuleStatusEnum;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/** @Audited 表达式使用的辅助函数，以 T(com.tapdata.tm.audit.AuditSpel).xxx(...) 调用。 */
@Component
public class AuditSpel {
	private static ModulesService modulesService;
	private static MongoTemplate mongoTemplate;

	public AuditSpel(ModulesService modulesService, MongoTemplate mongoTemplate) {
		AuditSpel.modulesService = modulesService;
		AuditSpel.mongoTemplate = mongoTemplate;
	}

	/** 用户编辑：accountStatus 由启用变为禁用记为禁用用户，反之记为启用用户，其余为编辑用户。 */
	public static String userUpdateAction(String userId, String settingJson) {
		try {
			Document body = Document.parse(settingJson);
			Object next = body.get("accountStatus");
			if (next != null && mongoTemplate != null && ObjectId.isValid(userId)) {
				Document current = mongoTemplate.findOne(Query.query(Criteria.where("_id").is(new ObjectId(userId))), Document.class, "User");
				Object previous = current == null ? null : current.get("accountStatus");
				int now = Integer.parseInt(String.valueOf(next));
				int before = previous == null ? -1 : Integer.parseInt(String.valueOf(previous));
				if (now == 0 && before != 0) {
					return AuditAction.USER_DISABLE.getKey();
				}
				if (now == 1 && before == 0) {
					return AuditAction.USER_ENABLE.getKey();
				}
			}
		} catch (Exception ignore) {
			// 按编辑记录
		}
		return AuditAction.USER_UPDATE.getKey();
	}

	/** 角色映射：涉及权限点为修改角色权限；给用户新增为授予角色，仅删除为撤销角色。 */
	public static String mappingAction(Collection<RoleMappingDto> adds, Collection<RoleMappingDto> deletes) {
		boolean permission = Stream.concat(stream(adds), stream(deletes)).anyMatch(m -> "PERMISSION".equals(m.getPrincipalType()));
		if (permission) {
			return AuditAction.PERMISSION_UPDATE.getKey();
		}
		return stream(adds).findAny().isPresent() ? AuditAction.ROLE_GRANT.getKey() : AuditAction.ROLE_REVOKE.getKey();
	}

	public static String mappingAction(RoleMappingDto mapping) {
		return mappingAction(mapping == null ? List.of() : List.of(mapping), List.of());
	}

	/** 角色映射涉及的对象：权限变更为角色，用户授予/撤销为用户。 */
	public static List<String> mappingObjects(Collection<RoleMappingDto> adds, Collection<RoleMappingDto> deletes) {
		return Stream.concat(stream(adds), stream(deletes))
				.map(m -> "PERMISSION".equals(m.getPrincipalType()) ? (m.getRoleId() == null ? null : m.getRoleId().toHexString()) : m.getPrincipalId())
				.filter(java.util.Objects::nonNull).distinct().toList();
	}

	private static Stream<RoleMappingDto> stream(Collection<RoleMappingDto> list) {
		return list == null ? Stream.empty() : list.stream().filter(java.util.Objects::nonNull);
	}

	/** 从 Loopback 风格的 where JSON 中取对象 ID：支持 id/_id 的字符串、{$oid}、{$in:[...]}。 */
	public static List<String> idFromWhere(String whereJson) {
		List<String> ids = new ArrayList<>();
		try {
			Document where = Document.parse(whereJson);
			Object value = where.containsKey("_id") ? where.get("_id") : where.get("id");
			collect(value, ids);
		} catch (Exception ignore) {
			// 不能解析时不记录对象 ID
		}
		return ids;
	}

	private static void collect(Object value, List<String> ids) {
		if (value instanceof Document doc) {
			if (doc.containsKey("$oid")) {
				ids.add(String.valueOf(doc.get("$oid")));
			} else if (doc.get("$in") instanceof Collection<?> in) {
				in.forEach(v -> collect(v, ids));
			} else if (doc.containsKey("$eq")) {
				collect(doc.get("$eq"), ids);
			}
		} else if (value != null) {
			ids.add(String.valueOf(value));
		}
	}

	/** Worker 通用更新：设置 isDeleted 为删除 Agent，设置 stopping 为停止服务；其余是引擎自身上报，不记录。 */
	public static String workerUpdateAction(String reqBody) {
		try {
			Document body = Document.parse(reqBody);
			Document set = body.get("$set") instanceof Document d ? d : body;
			if (Boolean.TRUE.equals(set.get("isDeleted"))) {
				return AuditAction.AGENT_DELETE.getKey();
			}
			if (Boolean.TRUE.equals(set.get("stopping"))) {
				return AuditAction.SERVICE_STOP.getKey();
			}
		} catch (Exception ignore) {
			// 无法解析时按非用户操作处理
		}
		return AuditAspect.SKIP;
	}

	/** API 服务编辑：待发布→已发布为发布，已发布→待发布为下线，其余为编辑（与接口权限判定一致）。 */
	public static String moduleAction(ModulesDto module) {
		try {
			if (module == null || module.getId() == null || modulesService == null) {
				return AuditAction.API_UPDATE.getKey();
			}
			ModulesDto current = modulesService.findById(module.getId(), new Field());
			if (current != null && ModuleStatusEnum.ACTIVE.getValue().equals(current.getStatus())
					&& ModuleStatusEnum.PENDING.getValue().equals(module.getStatus())) {
				return AuditAction.API_UNPUBLISH.getKey();
			}
			if (current != null && ModuleStatusEnum.PENDING.getValue().equals(current.getStatus())
					&& ModuleStatusEnum.ACTIVE.getValue().equals(module.getStatus())) {
				return AuditAction.API_PUBLISH.getKey();
			}
		} catch (Exception ignore) {
			// 按编辑记录
		}
		return AuditAction.API_UPDATE.getKey();
	}
}
