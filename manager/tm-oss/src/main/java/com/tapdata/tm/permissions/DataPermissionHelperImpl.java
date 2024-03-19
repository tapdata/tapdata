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

/**
 * All data permissions are processed by the current class
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/8 15:57 Create
 */
@Component
public class DataPermissionHelperImpl implements IDataPermissionHelper {
    private static final ThreadLocal<DataPermissionAuthInfoVo> THREAD_LOCAL = new ThreadLocal<>();

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
        DataPermissionAuthInfoVo authInfoVo = new DataPermissionAuthInfoVo()
                .id(id)
                .userId(userDetail.getUserId())
                .menuEnums(menuEnums)
                .dataTypeEnums(dataTypeEnums)
                .actionEnums(actionEnums)
                .created(System.currentTimeMillis());
        // check admin auth
        return authInfoVo.authWithAdmin();
    }

    public Set<String> mergeActions(Set<String> actions, Set<String> roleIds, List<DataPermissionAction> permissionItems) {
        if (null != permissionItems) {
            for (DataPermissionAction p : permissionItems) {
                if (DataPermissionTypeEnums.Role.name().equals(p.getType()) && roleIds.contains(p.getTypeId()) && null != p.getActions()) {
                    actions.addAll(p.getActions());
                }
            }
        }
        return actions;
    }

    public boolean setFilterConditions(boolean need2SetFilter, Query query, UserDetail userDetail) {
        DataPermissionAuthInfoVo vo = THREAD_LOCAL.get();
        return true;
    }

    public <E extends BaseEntity, D extends BaseDto> void convert(E entity, D dto) {
        DataPermissionAuthInfoVo vo = THREAD_LOCAL.get();
        if (null != vo && entity instanceof IDataPermissionEntity && dto instanceof IDataPermissionDto) {
            // menu and admin role auth not has role ids, set all actions
            dto.setPermissionActions(vo.getDataTypeEnums().allActions());
        }
    }

    public void cleanAuthOfRoleDelete(Set<String> roleIds) {
    }

    /**
     * @param userDetail     user info
     * @param menuEnums      menu type, check menu auth if not null
     * @param actionEnums    action type, not null
     * @param dataTypeEnums  data type, not null
     * @param id             data id, query and check data auth if not null
     * @param supplier       call in normal
     * @param noAuthSupplier call if data un auth
     * @param <T>            result type
     * @return call data
     */
    public <T> T check(
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

    public <T, D extends BaseDto> T checkOfQuery(
            UserDetail userDetail,
            DataPermissionDataTypeEnums dataTypeEnums,
            DataPermissionActionEnums actionEnums,
            Supplier<D> querySupplier,
            Function<D, DataPermissionMenuEnums> menuEnumsFun,
            Supplier<T> supplier,
            Supplier<T> unAuthSupplier
    ) {
        DataPermissionAuthInfoVo vo = new DataPermissionAuthInfoVo()
                .userId(userDetail.getUserId())
                .dataTypeEnums(dataTypeEnums)
                .actionEnums(actionEnums);
            return executeByAuthInfo(vo.authWithAdmin(), supplier);
    }

    public String signEncode(String currentId, String parentId) {
        return AES256Util.Aes256Encode(String.join(",", currentId, parentId));
    }

    public String signDecode(HttpServletRequest request, String id) {
        return null;
    }
}
