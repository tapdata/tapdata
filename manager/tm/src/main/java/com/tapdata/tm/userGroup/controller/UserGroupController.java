package com.tapdata.tm.userGroup.controller;

import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.userGroup.dto.UserGroupDto;
import com.tapdata.tm.userGroup.service.UserGroupService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;


/**
 * @Date: 2021/12/01
 * @Description:
 */
@Tag(name = "UserGroup", description = "UserGroup相关接口")
@RestController
@RequestMapping("/api/UserGroups")
public class UserGroupController extends BaseController {

    @Autowired
    private UserGroupService userGroupService;
    @Autowired
    private SettingsService settingsService;
    @Autowired
    private PermissionService permissionService;

    /**
     * Create a new instance of the model and persist it into the data source
     * @param userGroup
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<UserGroupDto> save(@RequestBody UserGroupDto userGroup) {
        if (settingsService.isCloud() || permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_USER_MANAGEMENT, getLoginUser().getUserId())) {
            userGroup.setId(null);
            return success(userGroupService.save(userGroup, getLoginUser()));
        } else {
            throw new BizException("NotAuthorized");
        }
    }

    /**
     *  Patch an existing model instance or insert a new one into the data source
     * @param userGroup
     * @return
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<UserGroupDto> update(@RequestBody UserGroupDto userGroup) {
        UserDetail userDetail = getLoginUser();
        return dataPermissionCheckOfMenu(userDetail, DataPermissionActionEnums.Edit,
                () -> success(userGroupService.save(userGroup, userDetail)));
    }


    /**
     * Find all instances of the model matched by filter from the data source
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<UserGroupDto>> find(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        UserDetail userDetail = getLoginUser();
        Filter userGroupFilter = filter;
        return DataPermissionMenuEnums.UserManagement.checkAndSetFilter(
                userDetail,
                DataPermissionActionEnums.View,
                () -> success(userGroupService.find(userGroupFilter, userDetail))
        );
    }

    /**
     *  Replace an existing model instance or insert a new one into the data source
     * @param userGroup
     * @return
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<UserGroupDto> put(@RequestBody UserGroupDto userGroup) {
        UserDetail userDetail = getLoginUser();
        return dataPermissionCheckOfMenu(userDetail, DataPermissionActionEnums.Edit,
                () -> success(userGroupService.replaceOrInsert(userGroup, userDetail)));
    }


    /**
     * Check whether a model instance exists in the data source
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        UserDetail userDetail = getLoginUser();
        return dataPermissionCheckOfId(userDetail, id, DataPermissionActionEnums.View, () -> {
            long count = userGroupService.count(Where.where("_id", MongoUtils.toObjectId(id)), userDetail);
            HashMap<String, Boolean> existsValue = new HashMap<>();
            existsValue.put("exists", count > 0);
            return success(existsValue);
        });
    }

    /**
     *  Patch attributes for a model instance and persist it into the data source
     * @param userGroup
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<UserGroupDto> updateById(@PathVariable("id") String id, @RequestBody UserGroupDto userGroup) {
        UserDetail userDetail = getLoginUser();
        return dataPermissionCheckOfId(userDetail, id, DataPermissionActionEnums.Edit, () -> {
            userGroup.setId(MongoUtils.toObjectId(id));
            return success(userGroupService.save(userGroup, userDetail));
        });
    }


    /**
     * Find a model instance by {{id}} from the data source
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<UserGroupDto> findById(@PathVariable("id") String id,
            @RequestParam(value = "fields", required = false) String fieldsJson) {
        Field fields = parseField(fieldsJson);
        UserDetail userDetail = getLoginUser();
        return dataPermissionCheckOfId(userDetail, id, DataPermissionActionEnums.View,
                () -> success(userGroupService.findById(MongoUtils.toObjectId(id), fields, userDetail)));
    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param userGroup
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PutMapping("{id}")
    public ResponseMessage<UserGroupDto> replceById(@PathVariable("id") String id, @RequestBody UserGroupDto userGroup) {
        UserDetail userDetail = getLoginUser();
        return dataPermissionCheckOfId(userDetail, id, DataPermissionActionEnums.Edit,
                () -> success(userGroupService.replaceById(MongoUtils.toObjectId(id), userGroup, userDetail)));
    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param userGroup
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PostMapping("{id}/replace")
    public ResponseMessage<UserGroupDto> replaceById2(@PathVariable("id") String id, @RequestBody UserGroupDto userGroup) {
        UserDetail userDetail = getLoginUser();
        return dataPermissionCheckOfId(userDetail, id, DataPermissionActionEnums.Edit,
                () -> success(userGroupService.replaceById(MongoUtils.toObjectId(id), userGroup, userDetail)));
    }



    /**
     * Delete a model instance by {{id}} from the data source
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Boolean> delete(@PathVariable("id") String id) {
        UserDetail userDetail = getLoginUser();
        return dataPermissionCheckOfId(userDetail, id, DataPermissionActionEnums.Delete,
                () -> success(userGroupService.deleteById(MongoUtils.toObjectId(id), userDetail)));
    }

    /**
     *  Check whether a model instance exists in the data source
     * @param id
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @GetMapping("{id}/exists")
    public ResponseMessage<HashMap<String, Boolean>> checkById1(@PathVariable("id") String id) {
        UserDetail userDetail = getLoginUser();
        return dataPermissionCheckOfId(userDetail, id, DataPermissionActionEnums.View, () -> {
            long count = userGroupService.count(Where.where("_id", MongoUtils.toObjectId(id)), userDetail);
            HashMap<String, Boolean> existsValue = new HashMap<>();
            existsValue.put("exists", count > 0);
            return success(existsValue);
        });
    }

    /**
     *  Count instances of the model matched by where from the data source
     * @param whereJson
     * @return
     */
    @Operation(summary = "Count instances of the model matched by where from the data source")
    @GetMapping("count")
    public ResponseMessage<HashMap<String, Long>> count(@RequestParam("where") String whereJson) {
        Where where = parseWhere(whereJson);
        if (where == null) {
            where = new Where();
        }
        UserDetail userDetail = getLoginUser();
        Where userGroupWhere = where;
        return DataPermissionMenuEnums.UserManagement.checkAndSetFilter(
                userDetail,
                DataPermissionActionEnums.View,
                () -> {
                    long count = userGroupService.count(userGroupWhere, userDetail);
                    HashMap<String, Long> countValue = new HashMap<>();
                    countValue.put("count", count);
                    return success(countValue);
                }
        );
    }

    /**
     *  Find first instance of the model matched by filter from the data source.
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<UserGroupDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        UserDetail userDetail = getLoginUser();
        Filter userGroupFilter = filter;
        return DataPermissionMenuEnums.UserManagement.checkAndSetFilter(
                userDetail,
                DataPermissionActionEnums.View,
                () -> success(userGroupService.findOne(userGroupFilter, userDetail))
        );
    }

    /**
     *  Update instances of the model matched by {{where}} from the data source.
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody UserGroupDto userGroup) {
        UserDetail userDetail = getLoginUser();
        return dataPermissionCheckOfMenu(userDetail, DataPermissionActionEnums.Edit, () -> {
            Where where = parseWhere(whereJson);
            long count = userGroupService.updateByWhere(where, userGroup, userDetail);
            HashMap<String, Long> countValue = new HashMap<>();
            countValue.put("count", count);
            return success(countValue);
        });
    }

    /**
     *  Update an existing model instance or insert a new one into the data source based on the where criteria.
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
    @PostMapping("upsertWithWhere")
    public ResponseMessage<UserGroupDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody UserGroupDto userGroup) {
        UserDetail userDetail = getLoginUser();
        return dataPermissionCheckOfMenu(userDetail, DataPermissionActionEnums.Edit, () -> {
            Where where = parseWhere(whereJson);
            return success(userGroupService.upsertByWhere(where, userGroup, userDetail));
        });
    }

    private <T> T dataPermissionCheckOfMenu(
            UserDetail userDetail,
            DataPermissionActionEnums action,
            Supplier<T> supplier
    ) {
        return DataPermissionHelper.check(
                userDetail,
                DataPermissionMenuEnums.UserManagement,
                action,
                DataPermissionDataTypeEnums.User,
                null,
                supplier,
                () -> dataPermissionUnAuth(action, Lists.newArrayList(action))
        );
    }

    private <T> T dataPermissionCheckOfId(
            UserDetail userDetail,
            String id,
            DataPermissionActionEnums action,
            Supplier<T> supplier
    ) {
        return DataPermissionHelper.checkOfQuery(
                userDetail,
                DataPermissionDataTypeEnums.User,
                action,
                userGroupService.dataPermissionFindById(MongoUtils.toObjectId(id), new Field()),
                dto -> DataPermissionMenuEnums.UserManagement,
                supplier,
                () -> dataPermissionUnAuth(action, Lists.newArrayList(action))
        );
    }

    private <T> T dataPermissionUnAuth(
            DataPermissionActionEnums action,
            List<DataPermissionActionEnums> need
    ) {
        throw new BizException(
                "insufficient.permissions",
                needAction(DataPermissionDataTypeEnums.User, Lists.newArrayList(action)),
                needAction(DataPermissionDataTypeEnums.User, need)
        );
    }

}
