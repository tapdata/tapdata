package com.tapdata.tm.permissions;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.base.DataPermissionAction;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
@Component
public class DataPermissionHelper {

    public static final String FIELD_NAME = "permissions";
    public static final String AUTH_TYPE = String.join(".", FIELD_NAME, "type");
    public static final String AUTH_TYPE_ID = String.join(".", FIELD_NAME, "typeId");
    public static final String AUTH_ACTIONS = String.join(".", FIELD_NAME, "actions");

    private static IDataPermissionHelper iDataPermissionHelper;

    public DataPermissionHelper(IDataPermissionHelper IDataPermissionHelper) {
        DataPermissionHelper.iDataPermissionHelper = IDataPermissionHelper;
    }


    public static Set<String> mergeActions(Set<String> actions, Set<String> roleIds, List<DataPermissionAction> permissionItems) {
        return iDataPermissionHelper.mergeActions(actions, roleIds, permissionItems);
    }

    public static boolean setFilterConditions(boolean need2SetFilter, Query query, UserDetail userDetail) {
        return iDataPermissionHelper.setFilterConditions(need2SetFilter, query, userDetail);
    }

    public static <E extends BaseEntity, D extends BaseDto> void convert(E entity, D dto) {
        iDataPermissionHelper.convert(entity, dto);
    }

    public static void cleanAuthOfRoleDelete(Set<String> roleIds) {
        iDataPermissionHelper.cleanAuthOfRoleDelete(roleIds);
    }

    public static <T> T check(
            UserDetail userDetail,
            DataPermissionMenuEnums menuEnums,
            DataPermissionActionEnums actionEnums,
            DataPermissionDataTypeEnums dataTypeEnums,
            String id,
            Supplier<T> supplier,
            Supplier<T> noAuthSupplier
    ) {
        return iDataPermissionHelper.check(userDetail, menuEnums, actionEnums, dataTypeEnums, id, supplier, noAuthSupplier);
    }

    public static <T, D extends BaseDto> T checkOfQuery(
            UserDetail userDetail,
            DataPermissionDataTypeEnums dataTypeEnums,
            DataPermissionActionEnums actionEnums,
            Supplier<D> querySupplier,
            Function<D, DataPermissionMenuEnums> menuEnumsFun,
            Supplier<T> supplier,
            Supplier<T> unAuthSupplier
    ) {
        return iDataPermissionHelper.checkOfQuery(userDetail, dataTypeEnums, actionEnums, querySupplier, menuEnumsFun, supplier, unAuthSupplier);
    }

    public static String signEncode(String currentId, String parentId) {
        return iDataPermissionHelper.signEncode(currentId, parentId);
    }

    public static String signDecode(HttpServletRequest request, String id) {
        return iDataPermissionHelper.signDecode(request, id);
    }
}
/*
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
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.permissions.constants.DataPermissionTypeEnums;
import com.tapdata.tm.permissions.service.DataPermissionService;
import com.tapdata.tm.permissions.vo.DataPermissionAuthInfoVo;
import com.tapdata.tm.permissions.vo.DataPermissionTypeVo;
import com.tapdata.tm.utils.AES256Util;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.tapdata.tm.permissions.DataPermissionHelper.*;

*//**
 * All data permissions are processed by the current class
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/8 15:57 Create
 *//*
public class DataPermissionHelper {

    public static final String FIELD_NAME = "permissions";
    public static final String AUTH_TYPE = String.join(".", FIELD_NAME, "type");
    public static final String AUTH_TYPE_ID = String.join(".", FIELD_NAME, "typeId");
    public static final String AUTH_ACTIONS = String.join(".", FIELD_NAME, "actions");
    private static final ThreadLocal<DataPermissionAuthInfoVo> THREAD_LOCAL = new ThreadLocal<>();

    private static boolean isAgentReq() {
        try {
            ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
            HttpServletRequest request = attributes.getRequest();
            String userAgent = request.getHeader("user-agent");
            return org.apache.commons.lang3.StringUtils.isNotBlank(userAgent) && (userAgent.contains("Java") || userAgent.contains("Node") || userAgent.contains("FlowEngine"));
        } catch (Exception ignore) {
            return false;
        }
    }

    private static boolean isUnCheckAuth(UserDetail userDetail) {
        return DataPermissionService.isCloud() || userDetail.isFreeAuth() || isAgentReq();
    }

    private static boolean isMenuAuth(DataPermissionMenuEnums menuEnums, Set<String> roleIdSet) {
        return DataPermissionService.getInstance().isMenuAuth(
                menuEnums.getAllDataPermissionName(), roleIdSet.stream().map(ObjectId::new).collect(Collectors.toSet())
        );
    }

    private static <T> T executeByAuthInfo(DataPermissionAuthInfoVo vo, Supplier<T> supplier) {
        try {
            THREAD_LOCAL.set(vo);
            return supplier.get();
        } finally {
            THREAD_LOCAL.remove();
        }
    }

    private static DataPermissionAuthInfoVo getAuthInfo(
            UserDetail userDetail,
            DataPermissionMenuEnums menuEnums,
            DataPermissionActionEnums actionEnums,
            DataPermissionDataTypeEnums dataTypeEnums,
            String id
    ) {
        if (isUnCheckAuth(userDetail)) return null;

        DataPermissionAuthInfoVo authInfoVo = new DataPermissionAuthInfoVo()
                .id(id)
                .userId(userDetail.getUserId())
                .menuEnums(menuEnums)
                .dataTypeEnums(dataTypeEnums)
                .actionEnums(actionEnums)
                .created(System.currentTimeMillis());

        // check admin auth
        if (userDetail.isRoot()) return authInfoVo.authWithAdmin();

        DataPermissionTypeVo[] typeVos = null;
        Set<String> roleSet = DataPermissionService.getInstance().getRoleIds(userDetail.getUserId());
        if (!(null == roleSet || roleSet.isEmpty())) {
            authInfoVo.setRoleSet(roleSet);

            // check menu auth
            if (null != menuEnums && isMenuAuth(menuEnums, roleSet)) return authInfoVo.authWithMenu();

            typeVos = new DataPermissionTypeVo[]{
                    new DataPermissionTypeVo(DataPermissionTypeEnums.Role, roleSet)
            };
        }

        // check data and role auth
        if (null != id) {
            return DataPermissionService.getInstance().setDataActions(authInfoVo, typeVos);
        }
        return authInfoVo;
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

    public static boolean setFilterConditions(boolean need2SetFilter, Query query, UserDetail userDetail) {
        DataPermissionAuthInfoVo vo = THREAD_LOCAL.get();
        if (null == vo || !need2SetFilter) return false;

        if (!vo.isSetQueryFilter()) return true;

        Set<String> roleIds = vo.getRoleSet();
        if (null == roleIds) return false;

        BaseRepository.removeFilter("user_id", query);
        try {
            if (!query.getFieldsObject().isEmpty()) {
                query.fields().include(FIELD_NAME);
            }
            BaseRepository.addOrFilter(query,
                    Criteria.where("user_id").is(userDetail.getUserId())
                    , Criteria.where(AUTH_TYPE).is(DataPermissionTypeEnums.Role)
                            .and(AUTH_TYPE_ID).in(roleIds)
                            .and(AUTH_ACTIONS).in(vo.getActionEnums())
            );
            return true;
        } catch (Exception e) {
            throw new RuntimeException("Set auth condition failed: " + e.getMessage(), e);
        }
    }

    public static <E extends BaseEntity, D extends BaseDto> void convert(E entity, D dto) {
        if (DataPermissionService.isCloud()) return;

        DataPermissionAuthInfoVo vo = THREAD_LOCAL.get();
        if (null != vo && entity instanceof IDataPermissionEntity && dto instanceof IDataPermissionDto) {
            // menu and admin role auth not has role ids, set all actions
            Set<String> roleIds = vo.getRoleSet();
            if (null == roleIds) {
                dto.setPermissionActions(vo.getDataTypeEnums().allActions());
                return;
            }

            // creator has all actions
            String userId = vo.getUserId();
            if (userId.equals(entity.getUserId())) {
                dto.setPermissionActions(vo.getDataTypeEnums().allActions());
                return;
            }

            Set<String> actions = new HashSet<>();
            mergeActions(actions, roleIds, ((IDataPermissionEntity) entity).getPermissions());
            dto.setPermissionActions(actions);
        }
    }

    public static void cleanAuthOfRoleDelete(Set<String> roleIds) {
        if (DataPermissionService.isCloud()) return;
        DataPermissionService.getInstance().cleanAuthOfRoleDelete(roleIds);
    }

    *//**
     * @param userDetail     user info
     * @param menuEnums      menu type, check menu auth if not null
     * @param actionEnums    action type, not null
     * @param dataTypeEnums  data type, not null
     * @param id             data id, query and check data auth if not null
     * @param supplier       call in normal
     * @param noAuthSupplier call if data un auth
     * @param <T>            result type
     * @return call data
     *//*
    public static  <T> T check(
            UserDetail userDetail,
            DataPermissionMenuEnums menuEnums,
            DataPermissionActionEnums actionEnums,
            DataPermissionDataTypeEnums dataTypeEnums,
            String id,
            Supplier<T> supplier,
            Supplier<T> noAuthSupplier
    ) {
        DataPermissionAuthInfoVo vo = getAuthInfo(userDetail, menuEnums, actionEnums, dataTypeEnums, id);
        if (null != vo) {
            if (vo.isUnAuth()) return noAuthSupplier.get();
            return executeByAuthInfo(vo, supplier);
        }
        return supplier.get();
    }

    public static  <T, D extends BaseDto> T checkOfQuery(
            UserDetail userDetail,
            DataPermissionDataTypeEnums dataTypeEnums,
            DataPermissionActionEnums actionEnums,
            Supplier<D> querySupplier,
            Function<D, DataPermissionMenuEnums> menuEnumsFun,
            Supplier<T> supplier,
            Supplier<T> unAuthSupplier
    ) {
        if (isUnCheckAuth(userDetail)) return supplier.get();

        DataPermissionAuthInfoVo vo = new DataPermissionAuthInfoVo()
                .userId(userDetail.getUserId())
                .dataTypeEnums(dataTypeEnums)
                .actionEnums(actionEnums);

        // check admin auth
        if (userDetail.isRoot()) {
            return executeByAuthInfo(vo.authWithAdmin(), supplier);
        }

        vo.setRoleSet(DataPermissionService.getInstance().getRoleIds(userDetail.getUserId()));

        D dto = executeByAuthInfo(vo, querySupplier);
        if (null == dto) return unAuthSupplier.get();

        vo.id(dto.getId().toHexString()).menuEnums(menuEnumsFun.apply(dto));

        if (userDetail.getUserId().equals(dto.getUserId())) {
            //check user auth
            return executeByAuthInfo(vo.authWithUserData(), supplier);
        } else if (null != dto.getPermissionActions() && dto.getPermissionActions().contains(actionEnums.name())) {
            //check role auth
            return executeByAuthInfo(vo.authWithUserData(), supplier);
        }

        // check menu auth
        if (null != vo.getMenuEnums() && isMenuAuth(vo.getMenuEnums(), vo.getRoleSet())) {
            return executeByAuthInfo(vo.authWithMenu(), supplier);
        }

        return unAuthSupplier.get();
    }

    public static String signEncode(String currentId, String parentId) {
        return AES256Util.Aes256Encode(String.join(",", currentId, parentId));
    }

    public static String signDecode(HttpServletRequest request, String id) {
        if (null == id) return null;

        String parentTaskSign = request.getHeader("parent_task_sign");
        if (null != parentTaskSign) {
            String str = AES256Util.Aes256Decode(parentTaskSign);
            if (!str.equals(parentTaskSign)) {
                String[] arr = str.split(",");
                if (arr.length == 2 && id.equals(arr[0])) return arr[1];
            }
        }
        return null;
    }
}*/

