package com.tapdata.tm.permissions;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.commons.base.DataPermissionAction;
import com.tapdata.tm.commons.base.IDataPermissionDto;
import com.tapdata.tm.commons.base.IDataPermissionEntity;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenu;
import com.tapdata.tm.permissions.constants.DataPermissionTypeEnums;
import com.tapdata.tm.permissions.service.DataPermissionService;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * All data permissions are processed by the current class
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/8 15:57 Create
 */
public class DataPermissionHelper {
	public static final String FIELD_NAME = "permissions";
	public static final String AUTH_TYPE = String.join(".", FIELD_NAME, "type");
	public static final String AUTH_TYPE_ID = String.join(".", FIELD_NAME, "typeId");
	private static final String AUTH_ACTIONS = String.join(".", FIELD_NAME, "actions");

	private static final ThreadLocal<Map<String, Object>> threadLocal = new ThreadLocal<>();
	private static final String THREAD_USER_ID = "THREAD_USER_ID";
	private static final String THREAD_ROLE_IDS = "THREAD_ROLE_IDS";
	private static final String THREAD_DATA_TYPE = "THREAD_DATA_TYPE";
	private static final String THREAD_SET_FILTER = "THREAD_SET_FILTER";
	private static final String THREAD_MENU_AUTH = "THREAD_MENU_AUTH";

	private static DataPermissionDataTypeEnums getDataType(Map<String, Object> m) {
		return (DataPermissionDataTypeEnums) m.get(THREAD_DATA_TYPE);
	}

	private static Set<String> getRoleIds(Map<String, Object> m) {
		return (Set<String>) m.get(THREAD_ROLE_IDS);
	}

	private static boolean getSetFilter(Map<String, Object> m) {
		return (boolean) m.get(THREAD_SET_FILTER);
	}

	private static void computePresent(Consumer<Map<String, Object>> fn) {
		Map<String, Object> map = threadLocal.get();
		if (null != map) {
			fn.accept(map);
		}
	}

	public static Set<String> mergeActions(Set<String> actions, Set<String> roleIds, List<DataPermissionAction> permissionItems) {
		if (null != permissionItems) {
			for (DataPermissionAction p : permissionItems) {
				if (DataPermissionTypeEnums.Role.name().equals(p.getType()) && roleIds.contains(p.getTypeId()) && null != p.getActions()) {
					actions.addAll(p.getActions());
				}
			}
		}
		return actions;
	}

	public static <T> T openInController(UserDetail userDetail, DataPermissionMenu dataPermissionMenu, boolean setFilter, Supplier<T> supplier) {
		if (DataPermissionService.isCloud() || userDetail.isFreeAuth()) {
			return supplier.get();
		}

		Map<String, Object> map = new HashMap<>();
		map.put(THREAD_DATA_TYPE, dataPermissionMenu.getDataType());
		map.put(THREAD_SET_FILTER, setFilter);

		// admin role has all actions
		if (!userDetail.isRoot()) {
			String uid = userDetail.getUserId();
			Set<String> roleIds = DataPermissionService.getInstance().getRoleIds(uid);
			if (!roleIds.isEmpty()) {

				// menu auth has all actions
				boolean menuAuth = DataPermissionService.getInstance().menuAuth(dataPermissionMenu.getAllDataPermissionName()
					, roleIds.stream().map(ObjectId::new).collect(Collectors.toSet())
				);
				map.put(THREAD_MENU_AUTH, menuAuth);
				if (!menuAuth) {
					map.put(THREAD_USER_ID, uid);
					map.put(THREAD_ROLE_IDS, roleIds);
				}
			}
		}

		try {
			threadLocal.set(map);
			return supplier.get();
		} finally {
			threadLocal.remove();
		}
	}

	public static boolean setFilterConditions(Query query, UserDetail userDetail) {
		Map<String, Object> m = threadLocal.get();
		if (null == m || !getSetFilter(m)) return false;

		if (Boolean.TRUE.equals(m.get(THREAD_MENU_AUTH))) {
			BaseRepository.removeFilter("user_id", query);
			return true;
		}

		Set<String> roleIds = getRoleIds(m);
		if (null == roleIds) return false;

		BaseRepository.removeFilter("user_id", query);
		try {
			BaseRepository.addOrFilter(query,
				Criteria.where("user_id").is(userDetail.getUserId())
				, Criteria.where(AUTH_TYPE).is(DataPermissionTypeEnums.Role)
					.and(AUTH_TYPE_ID).in(roleIds)
					.and(AUTH_ACTIONS).in(DataPermissionActionEnums.View)
			);
			return true;
		} catch (Exception e) {
			throw new RuntimeException("Set auth condition failed: " + e.getMessage(), e);
		}
	}

	public static <E extends BaseEntity, D extends BaseDto> void convert(E entity, D dto) {
		if (DataPermissionService.isCloud()) return;

		computePresent(currentMap -> {
			if (entity instanceof IDataPermissionEntity && dto instanceof IDataPermissionDto) {

				// menu and admin role auth not has role ids, set all actions
				Set<String> roleIds = getRoleIds(currentMap);
				if (null == roleIds) {
					dto.setPermissionActions(getDataType(currentMap).allActions());
					return;
				}

				// creator has all actions
				String userId = (String) threadLocal.get().get(THREAD_USER_ID);
				if (userId.equals(entity.getUserId())) {
					dto.setPermissionActions(getDataType(currentMap).allActions());
					return;
				}

				Set<String> actions = new HashSet<>();
				mergeActions(actions, roleIds, ((IDataPermissionEntity) entity).getPermissions());
				dto.setPermissionActions(actions);
			}
		});
	}

	public static void cleanAuthOfRoleDelete(Set<String> roleIds) {
		if (DataPermissionService.isCloud()) return;
		DataPermissionService.getInstance().cleanAuthOfRoleDelete(roleIds);
	}
}
