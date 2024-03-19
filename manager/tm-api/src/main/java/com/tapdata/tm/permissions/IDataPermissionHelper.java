package com.tapdata.tm.permissions;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.base.DataPermissionAction;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.permissions.vo.DataPermissionAuthInfoVo;
import com.tapdata.tm.utils.AES256Util;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public interface IDataPermissionHelper {

    Set<String> mergeActions(Set<String> actions, Set<String> roleIds, List<DataPermissionAction> permissionItems);

    boolean setFilterConditions(boolean need2SetFilter, Query query, UserDetail userDetail);

    <E extends BaseEntity, D extends BaseDto> void convert(E entity, D dto);

    void cleanAuthOfRoleDelete(Set<String> roleIds);

    <T> T check(
            UserDetail userDetail,
            DataPermissionMenuEnums menuEnums,
            DataPermissionActionEnums actionEnums,
            DataPermissionDataTypeEnums dataTypeEnums,
            String id,
            Supplier<T> supplier,
            Supplier<T> noAuthSupplier
    );

    <T, D extends BaseDto> T checkOfQuery(
            UserDetail userDetail,
            DataPermissionDataTypeEnums dataTypeEnums,
            DataPermissionActionEnums actionEnums,
            Supplier<D> querySupplier,
            Function<D, DataPermissionMenuEnums> menuEnumsFun,
            Supplier<T> supplier,
            Supplier<T> unAuthSupplier
    );

    String signEncode(String currentId, String parentId);

    String signDecode(HttpServletRequest request, String id);

}
