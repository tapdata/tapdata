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

    public DataPermissionHelper(IDataPermissionHelper helper) {
        DataPermissionHelper.iDataPermissionHelper = helper;
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
