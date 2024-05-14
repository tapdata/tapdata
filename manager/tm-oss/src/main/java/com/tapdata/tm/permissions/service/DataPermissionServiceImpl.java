package com.tapdata.tm.permissions.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.base.DataPermissionAction;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionTypeEnums;
import com.tapdata.tm.permissions.vo.DataPermissionAuthInfoVo;
import com.tapdata.tm.permissions.vo.DataPermissionTypeVo;
import com.tapdata.tm.roleMapping.dto.PrincipleType;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Supplier;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/8 15:58 Create
 */
@Service
public class DataPermissionServiceImpl extends DataPermissionService{
    public DataPermissionServiceImpl() {
        instance = this;
    }
    @Autowired
    private RoleMappingService roleMappingService;

    public Set<String> getRoleIds(String uid) {
        return new HashSet<>();
    }

    public boolean isMenuAuth(String menuName, Set<ObjectId> roleIds) {
        return roleMappingService.count(new Query()) > 0;
    }

    public Set<String> filterNotCreator(UserDetail userDetail, DataPermissionDataTypeEnums dataType, Set<ObjectId> dataIds) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    public Set<String> findDataActions(UserDetail userDetail, DataPermissionDataTypeEnums dataType, ObjectId dataId) {
        return dataType.allActions();
    }

    public Set<String> findDataActions(String userId, DataPermissionDataTypeEnums dataType, ObjectId dataId, Supplier<Set<String>> roleSetSupplier) {
        return dataType.allActions();
    }

    public List<DataPermissionAction> findDataPermissions(DataPermissionDataTypeEnums dataType, ObjectId dataId) {
        return null;
    }

    public DataPermissionAuthInfoVo setDataActions(DataPermissionAuthInfoVo authInfoVo, DataPermissionTypeVo... inTypes) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    public long saveDataPermissions(UserDetail userDetail, DataPermissionDataTypeEnums dataType, ObjectId dataId, List<DataPermissionAction> actions) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    public Set<String> findDataActionsOfType(DataPermissionTypeEnums type, String typeId, DataPermissionDataTypeEnums dataType, Set<ObjectId> dataIds) {
        return new HashSet<>();
    }

    public void cleanAuthOfRoleDelete(Set<String> roleIds) {
    }

    public void dataAuth(DataPermissionDataTypeEnums dataType, Set<ObjectId> dataIds, DataPermissionTypeEnums type, Set<String> typeIds, Set<String> actions) {
        throw new BizException("TapOssNonSupportFunctionException");
    }
}
