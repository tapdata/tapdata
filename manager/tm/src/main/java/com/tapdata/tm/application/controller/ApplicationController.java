package com.tapdata.tm.application.controller;

import com.google.common.collect.Sets;
import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.service.ApplicationService;
import com.tapdata.tm.application.vo.ModulePermissionVo;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.oauth2.service.MongoRegisteredClientRepository;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.oauth2.core.ClientAuthenticationMethod;

import org.springframework.security.oauth2.server.authorization.settings.ClientSettings;
import org.springframework.security.oauth2.server.authorization.settings.ConfigurationSettingNames;
import org.springframework.security.oauth2.server.authorization.settings.TokenSettings;
import org.springframework.web.bind.annotation.*;

import jakarta.servlet.http.HttpServletRequest;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;


/**
 * @Date: 2021/10/15
 * @Description:
 */
@Tag(name = "Application", description = "Applications 相关接口")
@RestController
@RequestMapping(value = {"/api/Applications"})
public class ApplicationController extends BaseController {

    @Autowired
    private ApplicationService applicationService;

    private <T> T dataPermissionUnAuth() {
        throw new RuntimeException("Un auth");
    }

    private <T> T dataPermissionCheckOfMenu(UserDetail userDetail, DataPermissionActionEnums actionEnums, Supplier<T> supplier) {
        return DataPermissionHelper.check(userDetail, DataPermissionMenuEnums.ApiClient, actionEnums, DataPermissionDataTypeEnums.Application, null, supplier, this::dataPermissionUnAuth);
    }

    private <T> T dataPermissionCheckOfId(HttpServletRequest request, UserDetail userDetail, String id, DataPermissionActionEnums actionEnums, Supplier<T> supplier) {
        String signId = id;
        if (request != null) {
            signId = Optional.ofNullable(DataPermissionHelper.signDecode(request, id)).orElse(id);
        }
        ObjectId applicationId = MongoUtils.toObjectId(signId);
        return DataPermissionHelper.checkOfQuery(
                userDetail,
                DataPermissionDataTypeEnums.Application,
                actionEnums,
                applicationService.dataPermissionFindById(applicationId, new Field()),
                dto -> DataPermissionMenuEnums.ApiClient,
                supplier,
                this::dataPermissionUnAuth
        );
    }

    /**
     * Create a new instance of the model and persist it into the data source
     *
     *
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<ApplicationDto> save(@RequestBody ApplicationDto applicationDto) {
        if (null != applicationDto && StringUtils.isNotBlank(applicationDto.getClientId())) {
            long count = applicationService.countOfClientId(applicationDto.getClientId(), null);
            if (count > 0) {
                throw new BizException("api.server.client.id.exists", applicationDto.getClientId());
            }
        }
        applicationDto.setId(null);
//        ObjectId objectId = new ObjectId();
//        metadataDefinition.setId(objectId);
//        metadataDefinition.setClientId(objectId.toHexString());
        if (CollectionUtils.isEmpty(applicationDto.getClientAuthenticationMethods())) {
            applicationDto.setClientAuthenticationMethods(
                    Sets.newHashSet(
//                            ClientAuthenticationMethod.POST.getValue(),
                            ClientAuthenticationMethod.CLIENT_SECRET_POST.getValue(),
                            ClientAuthenticationMethod.CLIENT_SECRET_JWT.getValue(),
                            ClientAuthenticationMethod.PRIVATE_KEY_JWT.getValue())
            );
        }

        if (StringUtils.isBlank(applicationDto.getTokenSettings())) {
            Map<String, Object> settings = new HashMap<>();
            settings.put(ConfigurationSettingNames.Token.ACCESS_TOKEN_TIME_TO_LIVE, Duration.ofDays(14));
            TokenSettings tokenSettings = TokenSettings.builder().
                    accessTokenTimeToLive(Duration.ofDays(14)).
                    refreshTokenTimeToLive(Duration.ofDays(14)).
                    reuseRefreshTokens(true)
                    .build();
            applicationDto.setTokenSettings(MongoRegisteredClientRepository.writeMap(tokenSettings.getSettings()));
        }

        if (StringUtils.isBlank(applicationDto.getClientSettings())) {
            ClientSettings clientSettings = ClientSettings.builder().requireAuthorizationConsent(true).build();
            applicationDto.setClientSettings(MongoRegisteredClientRepository.writeMap(clientSettings.getSettings()));
        }

        ApplicationDto dto = applicationService.save(applicationDto, getLoginUser());
        // 只有当 clientId 为空时，才设置为 id 的十六进制字符串
        if (StringUtils.isBlank(dto.getClientId())) {
            dto.setClientId(dto.getId().toHexString());
        }
        applicationService.updateById(dto, getLoginUser());
        return success(dto);
    }

    /**
     * 分页返回
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<ApplicationDto>> find(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        Where where = filter.getWhere();
        if (where == null) {
            where = new Where();
            filter.setWhere(where);
        }
        Document document = new Document();
        document.put("$ne", true);
        where.putIfAbsent("is_deleted", document);

        Filter finalFilter = filter;
        UserDetail userDetail = getLoginUser();
        return success(dataPermissionCheckOfMenu(userDetail, DataPermissionActionEnums.View,
                () -> applicationService.find(finalFilter, userDetail)));
    }


    /**
     * Patch attributes for a model instance and persist it into the data source
     *
     * @param metadataDefinition
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping
    public ResponseMessage<ApplicationDto> updateById(HttpServletRequest request, @RequestBody ApplicationDto metadataDefinition) {
        UserDetail userDetail = getLoginUser();
        return success(dataPermissionCheckOfId(request, userDetail, metadataDefinition.getId().toHexString(), DataPermissionActionEnums.Edit,
                () -> applicationService.updateById(metadataDefinition, userDetail)));
    }


    /**
     * Find a model instance by {{id}} from the data source
     *
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<ApplicationDto> findById(HttpServletRequest request, @PathVariable("id") String id,
                                                    @RequestParam("fields") String fieldsJson) {
        Field fields = parseField(fieldsJson);
        UserDetail userDetail = getLoginUser();
        return success(dataPermissionCheckOfId(request, userDetail, id, DataPermissionActionEnums.View,
                () -> applicationService.findById(MongoUtils.toObjectId(id), fields, userDetail)));
    }


    /**
     * Delete a model instance by {{id}} from the data source
     *
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(HttpServletRequest request,@PathVariable("id") String id) {
        UserDetail userDetail = getLoginUser();
        return success(dataPermissionCheckOfId(request, userDetail, id, DataPermissionActionEnums.Delete, () -> {
            applicationService.deleteLogicsById(id);
            return null;
        }));
    }


    /**
     *  Count instances of the model matched by where from the data source
     * @param whereJson
     * @return
     */
  /*  @Operation(summary = "Count instances of the model matched by where from the data source")
    @GetMapping("count")
    public ResponseMessage<HashMap<String, Long>> count(@RequestParam("where") String whereJson) {
        Where where = parseWhere(whereJson);
        if (where == null) {
            where = new Where();
        }
        long count = applicationService.count(where, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }*/

    /**
     * Find first instance of the model matched by filter from the data source.
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<ApplicationDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        Filter finalFilter = filter;
        UserDetail userDetail = getLoginUser();
        return success(dataPermissionCheckOfMenu(userDetail, DataPermissionActionEnums.View,
                () -> applicationService.findOne(finalFilter, userDetail)));
    }

    /**
     * Update instances of the model matched by {{where}} from the data source.
     *
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody ApplicationDto metadataDefinition) {
        Where where = parseWhere(whereJson);
        UserDetail userDetail = getLoginUser();
        long count = dataPermissionCheckOfMenu(userDetail, DataPermissionActionEnums.Edit,
                () -> applicationService.updateByWhere(where, metadataDefinition, userDetail));
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    /**
     * Update an existing model instance or insert a new one into the data source based on the where criteria.
     *
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
    @PostMapping("upsertWithWhere")
    public ResponseMessage<ApplicationDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody ApplicationDto metadataDefinition) {
        Where where = parseWhere(whereJson);
        UserDetail userDetail = getLoginUser();
        return success(dataPermissionCheckOfMenu(userDetail, DataPermissionActionEnums.Edit,
                () -> applicationService.upsertByWhere(where, metadataDefinition, userDetail)));
    }

    /**
     * Get modules that the application has permission to access
     *
     * @param id Application ID
     * @return List of modules with id and name only
     */
    @Operation(summary = "Get modules that the application has permission to access")
    @GetMapping("{id}/modules")
    public ResponseMessage<List<ModulePermissionVo>> getAccessibleModules(HttpServletRequest request, @PathVariable("id") String id) {
        UserDetail userDetail = getLoginUser();
        return success(dataPermissionCheckOfId(request, userDetail, id, null,
                () -> applicationService.getAccessibleModules(id, userDetail)));
    }

}
