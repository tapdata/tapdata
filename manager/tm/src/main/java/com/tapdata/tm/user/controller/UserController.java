package com.tapdata.tm.user.controller;

import cn.hutool.crypto.digest.BCrypt;
import com.mongodb.BasicDBObject;
import com.tapdata.tm.Permission.dto.PermissionDto;
import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.dto.TestResponseDto;
import com.tapdata.tm.accessToken.dto.AccessTokenDto;
import com.tapdata.tm.accessToken.dto.AuthType;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatadefinition.param.BatchUpdateParam;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import com.tapdata.tm.role.dto.RoleDto;
import com.tapdata.tm.role.service.RoleService;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import com.tapdata.tm.user.dto.*;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.user.param.ResetPasswordParam;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.user.dto.TestLdapDto;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.RC4Util;
import com.tapdata.tm.utils.SendStatus;
import com.tapdata.tm.utils.WebUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;
import java.util.stream.Collectors;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/11 6:54 上午
 * @description
 */
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/users")
@Slf4j
@Tag(name = "User", description = "用户信息接口")
@ApiResponses(value = {@ApiResponse(description = "successful operation", responseCode = "200")})
public class UserController extends BaseController {

    @Autowired
    UserService userService;

    @Autowired
    AccessTokenService accessTokenService;

    @Autowired
    PermissionService permissionService;

    @Autowired
    RoleMappingService roleMappingService;

    @Autowired
    private RoleService roleService;

    @Autowired
    UserLogService userLogService;

    @Value("#{'${spring.profiles.include:idaas}'.split(',')}")
    private List<String> productList;
    @Autowired
    @Qualifier("caffeineCache")
    private CaffeineCacheManager userCache;
    @Autowired
    private MetadataDefinitionService metadataDefinitionService;


    private static final String RC4_KEY = "Gotapd8";

    /**
     * 获取用户信息
     *
     * @return
     */
    @GetMapping("/self")
    public ResponseMessage<UserDto> self() {
        /*UserInfoVo userInfoVo = userService.self(getLoginUser());
        return success(userInfoVo);*/

        UserDto userDto = userService.findById(toObjectId(getLoginUser().getUserId()));
        return success(userDto);
    }

    /**
     * 更新用户信息
     *
     * @return
     */
    @PatchMapping("{id}")
    public ResponseMessage<UserDto> updateUserInfo(@PathVariable("id") String id,
                                                   @RequestBody String settingJson) {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
        HttpServletRequest request = attributes.getRequest();
        Locale locale = WebUtils.getLocale(request);
        return success(userService.updateUserSetting(id, settingJson, getLoginUser(),locale));
    }

    /**
     * 生成用户token
     *
     * @return
     */
    @PostMapping("generatetoken")
    public ResponseMessage<AccessTokenDto> generateToken(@RequestBody GenerateAccessTokenDto accessCodeDto) {
        String accessCode = accessCodeDto.getAccesscode();
        if (StringUtils.isEmpty(accessCode)) {
            return failed("AccessCode.Is.Null");
        }
        AccessTokenDto generateToken = accessTokenService.generateToken(accessCodeDto);
        if (null == generateToken) {
            return failed("AccessCode.No.User");
        } else {
            return success(generateToken);
        }
    }

    /**
     * tapdataegine
     * 用这个方法获取token
     *
     * @param request
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "generatetoken",
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    @ResponseBody
    public ResponseMessage<AccessTokenDto> generatetoken(HttpServletRequest request) throws IOException {
        String accessCode = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
        //传过来的的  accesscode形式位： accesscode=3324cfdf-7d3e-4792-bd32-571638d4562f
        accessCode = accessCode.replace("accesscode=", "");

        if (StringUtils.isEmpty(accessCode)) {
            return failed("AccessCode.Is.Null");
        }
        AccessTokenDto generateToken = accessTokenService.generateToken(accessCode);
        if (null == generateToken) {
            return failed("AccessCode.No.User");
        } else {
            return success(generateToken);
        }
    }


    @GetMapping("byToken")
    public ResponseMessage<UserDto> getUserByToken() {
        UserDetail user = getLoginUser();
        return getUser(user.getUserId());
    }


    /**
     * 根据userID 查询用户信息
     *
     * @return
     */
    @GetMapping("{userId}")
    public ResponseMessage<UserDto> getUser(@PathVariable(value = "userId") String userId) {
        if (productList != null && productList.contains("dfs")) {
            return success(Optional.ofNullable(userCache.getCache("userCache"))
                    .map(cache -> cache.get(userId, () -> userService.getUserDetail(userId)))
                    .orElseGet(() -> userService.getUserDetail(userId)));
        }
        return success(userService.getUserDetail(userId));
    }



    /**
     * 根据userID 查询用户权限
     *
     * @return
     */
    @GetMapping("{userId}/permissions")
    public ResponseMessage<Map<String, List>> permissions(@PathVariable(value = "userId") String userId) {

        List<PermissionDto> permissionDtoList = permissionService.getCurrentPermission(userId);
        return success(new HashMap<String, List>() {{
            put("permissions", permissionDtoList);
        }});
    }

    /**
     * Find all instances of the model matched by filter from the data source
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<UserDto>> find(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        //用逻辑删除
        Where where=filter.getWhere();
        Map notDeleteMap = new HashMap();
        notDeleteMap.put("$ne", true);
        where.put("isDeleted", notDeleteMap);
        Page<UserDto> userDtoPage = userService.find(filter, getLoginUser());
        List<UserDto> items = userDtoPage.getItems();
        if (CollectionUtils.isNotEmpty(items)) {
            List<String> ids = items.stream().map(userDto -> userDto.getId().toHexString()).collect(Collectors.toList());
            List<RoleMappingDto> roleMappingDtos = roleMappingService.findAll(Query.query(Criteria.where("principalId").in(ids)));
            if (CollectionUtils.isNotEmpty(roleMappingDtos)) {
                List<ObjectId> objectIds = roleMappingDtos.stream().map(RoleMappingDto::getRoleId).collect(Collectors.toList());
                List<RoleDto> roleDtos = roleService.findAll(Query.query(Criteria.where("_id").in(objectIds)));
                if (CollectionUtils.isNotEmpty(roleDtos)) {
                    roleMappingDtos.forEach(roleMappingDto -> roleDtos.stream()
                            .filter(roleDto -> roleDto.getId().toHexString().equals(roleMappingDto.getRoleId().toHexString()))
                            .findFirst().ifPresent(roleMappingDto::setRole));
                }
                items.forEach(userDto -> {
                    if (userDto.getRoleMappings() == null) {
                        userDto.setRoleMappings(new ArrayList<>());
                    }
                    roleMappingDtos.stream()
                            .filter(roleMappingDto -> roleMappingDto.getPrincipalId().equals(userDto.getId().toHexString()))
                            .forEach(roleMappingDto -> userDto.getRoleMappings().add(roleMappingDto));
                });
            }
        }
        return success(userDtoPage);
    }

    /**
     * Find first instance of the model matched by filter from the data source.
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("/findOne")
    public ResponseMessage<UserDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(userService.findOne(filter, getLoginUser()));
    }


    /**
     * 根据邮箱和密码登录
     * 密码需要先进行RC4解密，解密后经过sha256加密后与数据库用户密码匹配
     **/
    @Operation(summary = "user login")
    @PostMapping("/login")
    public ResponseMessage<AccessTokenDto> login(@RequestBody @Validated LoginRequest loginRequest) {
        String password;
        try {
            password = RC4Util.decrypt(RC4_KEY, loginRequest.getPassword());
        } catch (Exception e) {
            return failed(e);
        }
        User user;
        // 从setting中获取是否开启AD
        boolean adLoginEnable = userService.checkLdapLoginEnable();
        //若开启，走AD校验，校验通过创建用户，未通过登录失败
        if (adLoginEnable && !"admin@admin.com".equals(loginRequest.getEmail())) {
            boolean login = userService.loginByLdap(loginRequest.getEmail(), password);
            if (!login) {
                throw new BizException(userService.checkLoginBriefTipsEnable("Incorrect.Password"));
            }
            //登录成功查询用户是否存在没有就创建用户
            if (StringUtils.isNotBlank(loginRequest.getEmail())) {
                String[] split = loginRequest.getEmail().split("@");
                if (split != null && split.length > 1) {
                    loginRequest.setEmail(split[0]);
                }
            }
            UserDto adUser = userService.findOne(new Query(Criteria.where("ldapAccount").regex(loginRequest.getEmail(), "i").and("source").is("createLdap")
                    .orOperator(Criteria.where("isDeleted").is(false), Criteria.where("isDeleted").exists(false))));
            if (null == adUser) {
                CreateUserRequest request = new CreateUserRequest();
                request.setLdapAccount(loginRequest.getEmail());
                request.setUsername(loginRequest.getEmail());
                request.setAccesscode("");
                request.setAccountStatus(1);
                request.setPassword(password);
                request.setSource("createLdap");
                request.setEmailVerified(true);
                List<Object> roleusers = new ArrayList<>();
                roleusers.add("5d31ae1ab953565ded04badd");
                request.setRoleusers(roleusers);
                UserDetail userDetail = userService.loadUserByUsername("admin@admin.com");
                adUser = userService.save(request, userDetail);
            }
            user = userService.convertToEntity(User.class, adUser);
            user.setPassword(BCrypt.hashpw(password));
        } else {
            //未开启走原来的逻辑
            user = userService.findOneByEmail(loginRequest.getEmail());
        }
        if (user.getAccountStatus() == 2) {
            return failed("110500", user.isEmailVerified() ? "WAITING_APPROVE" : "EMAIL_NON_VERIFLED");
        } else if (user.getAccountStatus() == 0) {
            return failed("Account.disabled");
        }
        boolean timesOverFive = user.getLoginTimes() != null && user.getLoginTimes() >= 5;
        boolean timeInTenMin = user.getLoginTime() != null
                && user.getLoginTime().getTime() + 1000 * 60 * 10 > System.currentTimeMillis();
        if (timesOverFive && timeInTenMin) {
            throw new BizException("Too.Many.Login.Failures");
        }
        AccessTokenDto accessTokenDto;
        if (StringUtils.isNotBlank(password) && BCrypt.checkpw(password, user.getPassword())) {
            if (user.getLoginTimes() != null && user.getLoginTimes() > 0) {
                userService.update(Query.query(Criteria.where("id").is(user.getId())), Update.update("loginTimes", 0));
            }
            boolean loginSingleSessionEnable = userService.checkLoginSingleSessionEnable();
            if (loginSingleSessionEnable) {
                accessTokenService.removeAccessTokenByAuthType(user.getId(), AuthType.USERNAME_LOGIN.getValue());
            }
            accessTokenDto = accessTokenService.save(user);
        } else {
            Update update = Update.update("loginTimes", user.getLoginTimes() != null ? (user.getLoginTimes() % 6 + 1) : 1).set("loginTime", new Date());
            userService.update(Query.query(Criteria.where("id").is(user.getId())), update);
            throw new BizException(userService.checkLoginBriefTipsEnable("Incorrect.Password"));
        }
        try {
            userLogService.addUserLog(Modular.SYSTEM, com.tapdata.tm.userLog.constant.Operation.LOGIN, user.getId().toHexString(), "", "");
        } catch (Exception e) {
            log.error("登录添加操作日志异常 {}", e.getMessage());
        }
        return success(accessTokenDto);

    }

    @Operation(summary = "User logout")
    @PostMapping("/logout")
    public ResponseMessage<Long> logout(HttpServletRequest request) {
        if ((request.getQueryString() != null ? request.getQueryString() : "").contains("access_token")) {

            Map<String, String> queryMap = Arrays.stream(request.getQueryString().split("&"))
                    .filter(s -> s.startsWith("access_token"))
                    .map(s -> s.split("=")).collect(Collectors.toMap(a -> a[0], a -> {
                        try {
                            return URLDecoder.decode(a[1], "UTF-8");
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                            return a[1];
                        }
                    }, (a, b) -> a));
            String accessToken = queryMap.get("access_token");
            if (StringUtils.isNotBlank(accessToken)) {
                return success(accessTokenService.removeAccessToken(accessToken, getLoginUser()));
            }
        }
        return success();
    }

    @Operation(summary = "user roles")
    @GetMapping("/roles")
    public ResponseMessage<Page<RoleDto>> roles(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        UserDetail userDetail = getLoginUser();
        if (!"admin@admin.com".equals(userDetail.getEmail())) {
            filter.getWhere().and("name", new BasicDBObject().append("$ne", "admin"));
        }
        Page<RoleDto> roleDtoPage = roleService.find(filter);
        List<RoleDto> items = roleDtoPage.getItems();
        if (CollectionUtils.isNotEmpty(items)) {
            List<ObjectId> userIds = items.stream().map(item -> toObjectId(item.getUserId())).collect(Collectors.toList());
            List<UserDto> userDtos = userService.findAll(Query.query(Criteria.where("_id").in(userIds)));
            items.forEach(item -> {
                userDtos.stream().filter(userDto -> userDto.getId().toHexString().equals(item.getUserId()))
                        .findFirst()
                        .ifPresent(userDto -> item.setUserEmail(userDto.getEmail()));
                long count = roleMappingService.count(Query.query(Criteria.where("principalType").is("USER").and("roleId").is(item.getId())));
                item.setUserCount(count);
            });
        }
        return success(roleDtoPage);

    }

    @Operation(summary = "user deletePermissionRoleMapping")
    @DeleteMapping("/deletePermissionRoleMapping")
    public ResponseMessage<Page<RoleDto>> deletePermissionRoleMapping(@RequestParam(value = "id") String id, @RequestBody DeletePermissionRoleMappingDto dto) {

        UserDetail userDetail = getLoginUser();
        roleMappingService.deleteAll(Query.query(Criteria.where("roleId").is(toObjectId(id)).and("principalType").is("PERMISSION")));
        if (CollectionUtils.isNotEmpty(dto.getData())) {
            roleMappingService.save(dto.getData(), userDetail);
        }
        return success();

    }

    @Operation(summary = "user updatePermissionRoleMapping")
    @PutMapping("/updatePermissionRoleMapping")
    public ResponseMessage<Page<RoleDto>> updatePermissionRoleMapping(@RequestBody UpdatePermissionRoleMappingDto dto) {
        userService.updatePermissionRoleMapping(dto, getLoginUser());

        return success();

    }

    @Operation(summary = "Count instances of the model matched by where from the data source")
    @GetMapping("/count")
    public ResponseMessage<Object> count(
            @Parameter(in = ParameterIn.QUERY, description = "Criteria to match model instances")
            @RequestParam(value = "where", required = false) String whereJson) {
        Filter filter = parseFilter(whereJson);
        long count = userService.count(filter.getWhere(), getLoginUser());
        return success(new HashMap<String, Long>() {{
            put("count", count);
        }});
    }

    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody UserDto userDto) {
        Where where = parseWhere(whereJson);
        long count = userService.updateByWhere(where, userDto, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<UserDto> save(@RequestBody @Validated CreateUserRequest request) {
        if ((productList != null && productList.contains("dfs")) ||
                Boolean.TRUE.equals(permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_USER_MANAGEMENT, getLoginUser().getUserId()))) {
            return success(userService.save(request, getLoginUser()));
        } else {
            throw new BizException("NotAuthorized");
        }
    }

    @Operation(summary = "User change password")
    @PostMapping("/change-password")
    public ResponseMessage<Long> changePassword(@RequestBody @Validated ChangePasswordRequest request) {
        return success(userService.changePassword(request, getLoginUser()));
    }

    @Operation(summary = "Bind phone on current login user")
    @PatchMapping("/phone")
    public ResponseMessage<UserDto> updatePhone(@RequestBody @Validated BindPhoneReq bindPhoneReq) {
        return success(userService.updatePhone(getLoginUser(), bindPhoneReq));
    }
    @Operation(summary = "Bind email on current login user")
    @PatchMapping("/email")
    public ResponseMessage<UserDto> updatePhone(@RequestBody @Validated BindEmailReq bindEmailReq) {
        return success(userService.updateEmail(getLoginUser(), bindEmailReq));
    }


    @Operation(summary = "企业版重置密码,发送验证码")
    @PostMapping("/sendValidateCode")
    public ResponseMessage sendValidateCode(@RequestBody ResetPasswordParam resetPasswordParam) {
        String email = resetPasswordParam.getEmail();
        if (StringUtils.isEmpty(email)) {
            throw new BizException("Email.Not.Exist");
        }

        User user = userService.findOneByEmail(email);
        if (null == user) {
            throw new BizException("User.Not.Exist");
        }
        SendStatus sendResult = userService.sendValidateCde(email);
        if (!"true".equals(sendResult.getStatus())){
            throw new BizException("Email.verify.code.Send.fail", sendResult.getErrorMessage());
        }
        return success();
    }

    @Operation(summary = "企业版重置密码")
    @PostMapping("/reset")
    public ResponseMessage<Long> reset(@RequestBody ResetPasswordParam resetPasswordParam) {
        return success(userService.reset(resetPasswordParam));
    }

    @Operation(summary = "企业版重置密码")
    @DeleteMapping("{id}")
    public ResponseMessage<Long> delete(@PathVariable("id") String id) {
        userService.delete(id, getLoginUser());
        return success();
    }

    @Operation(summary = "test ldap login")
    @PostMapping("testLdapLogin")
    public ResponseMessage<TestResponseDto> testLoginByLdap(@RequestBody TestLdapDto testldapDto) {
        TestResponseDto res = userService.testLoginByLdap(testldapDto);
        return success(res);
    }

    @Operation(summary = "check ldap login enable")
    @GetMapping("checkLdapLoginEnable")
    public ResponseMessage<Boolean> checkADLoginEnable() {
        Boolean res = userService.checkLdapLoginEnable();
        return success(res);
    }

    @Operation(summary = "refresh access code")
    @PostMapping("refreshAccessCode")
    public ResponseMessage<String> refreshAccessCode() {
        return success(userService.refreshAccessCode(getLoginUser()));
    }

    @Operation(summary = " 批量修改所属类别")
    @PatchMapping("batchUpdateListtags")
    public ResponseMessage<String> batchUpdateListTags( @RequestBody BatchUpdateParam batchUpdateParam) {
        List<String> idList = batchUpdateParam.getId();
        List<com.tapdata.tm.commons.schema.Tag> listTags = batchUpdateParam.getListtags();
        Update update = new Update().set("listtags", listTags);
        userService.update(new Query(Criteria.where("id").in(idList)), update, getLoginUser());
        return success();
    }
}
