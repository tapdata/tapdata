package com.tapdata.tm.permissions;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.base.DataPermissionAction;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.utils.AES256Util;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public interface IDataPermissionHelper {

    default Set<String> mergeActions(Set<String> actions, Set<String> roleIds, List<DataPermissionAction> permissionItems) {
        return actions;
    }

    default boolean setFilterConditions(boolean need2SetFilter, Query query, UserDetail userDetail) {
        return false;
    }

    default <E extends BaseEntity, D extends BaseDto> void convert(E entity, D dto) {
        return;
    }

    default void cleanAuthOfRoleDelete(Set<String> roleIds) {
        return;
    }

    default <T> T check(
            UserDetail userDetail,
            DataPermissionMenuEnums menuEnums,
            DataPermissionActionEnums actionEnums,
            DataPermissionDataTypeEnums dataTypeEnums,
            String id,
            Supplier<T> supplier,
            Supplier<T> noAuthSupplier
    ) {
        return supplier.get();
    }

    default <T, D extends BaseDto> T checkOfQuery(
            UserDetail userDetail,
            DataPermissionDataTypeEnums dataTypeEnums,
            DataPermissionActionEnums actionEnums,
            Supplier<D> querySupplier,
            Function<D, DataPermissionMenuEnums> menuEnumsFun,
            Supplier<T> supplier,
            Supplier<T> unAuthSupplier
    ) {
        return supplier.get();
    }

    default String signEncode(String currentId, String parentId) {
        return AES256Util.Aes256Encode(String.join(",", currentId, parentId));
    }

    default String signDecode(HttpServletRequest request, String id) {
        return null;
    }
    default void test1(){
        System.err.println("11111");
    };
}
