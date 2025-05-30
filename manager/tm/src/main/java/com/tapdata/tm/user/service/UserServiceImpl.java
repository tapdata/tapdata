package com.tapdata.tm.user.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.crypto.digest.BCrypt;
import cn.hutool.json.JSONObject;
import com.google.common.collect.Sets;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Permission.entity.PermissionEntity;
import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.Settings.dto.TestResponseDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.function.Bi3Consumer;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.customer.dto.CustomerDto;
import com.tapdata.tm.customer.service.CustomerService;
import com.tapdata.tm.role.dto.RoleDto;
import com.tapdata.tm.role.service.RoleService;
import com.tapdata.tm.roleMapping.dto.PrincipleType;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import com.tapdata.tm.task.service.LdpService;
import com.tapdata.tm.tcm.dto.UserInfoDto;
import com.tapdata.tm.tcm.service.TcmService;
import com.tapdata.tm.user.dto.*;
import com.tapdata.tm.user.entity.*;
import com.tapdata.tm.user.param.ResetPasswordParam;
import com.tapdata.tm.user.repository.UserRepository;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.MailUtils;
import com.tapdata.tm.utils.SendStatus;
import com.tapdata.tm.utils.UUIDUtil;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import javax.net.ssl.*;
import java.io.*;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/10 8:41 下午
 * @description
 */
@Component
@Slf4j
public class UserServiceImpl extends UserService{
    public UserServiceImpl(@NonNull UserRepository repository) {
        super(repository);
    }
    @Value("${spring.data.mongodb.default.uri}")
    private String mongodbUri;
    @Value("${server.port}")
    private String serverPort;
    @Value("${spring.data.mongodb.ssl}")
    private String ssl;
    @Value("${spring.data.mongodb.caPath}")
    private String caPath;
    @Value("${spring.data.mongodb.keyPath}")
    private String keyPath;
    @Autowired
    TcmService tcmService;

    @Autowired
    RoleMappingService roleMappingService;

    @Autowired
    private RoleService roleService;
    @Lazy
    @Autowired
    private UserLogService userLogService;

    @Autowired
    private CustomerService customerService;

    @Autowired
    PermissionService permissionService;

    private final String DEFAULT_MAIL_SUFFIX = "@custom.com";

    @Autowired
    MailUtils mailUtils;
    @Lazy
    @Autowired
    private LdpService ldpService;
    @Autowired
    private SettingsService settingsService;

    @Override
    protected void beforeSave(UserDto dto, UserDetail userDetail) {

    }

    /**
     * 根据用户名称加载用户
     *
     * @param username
     * @return
     */
    public UserDetail loadUserByUsername(String username) {
        Optional<User> userOptional = repository.findOne(
                Query.query(new Criteria().orOperator(
                        Criteria.where("username").is(username),
                        Criteria.where("email").is(username))));
        if (userOptional.isPresent()) {
            User user = userOptional.get();

            return getUserDetail(user);
        }
        return null;
    }


    public List<UserDetail> loadAllUser() {
        List<User> all = repository.findAll(new Query());
        if (CollectionUtils.isNotEmpty(all)) {
            return all.stream().map(this::getUserDetail).collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    /**
     * 暂时这么用，等用户角色权限需求完善了 再调整
     *
     * @param user
     * @return
     */
    @NotNull
    protected UserDetail getUserDetail(User user) {
        Set<SimpleGrantedAuthority> roleList = Sets.newHashSet();
        roleList.add(new SimpleGrantedAuthority("USERS"));
        if (!Objects.isNull(user.getRole()) && user.getRole() == 1) {
            roleList.add(new SimpleGrantedAuthority("ADMIN"));
        }

        return new UserDetail(user, roleList);
    }

    public Map<String, UserDetail> getUserMapByIdList(List<String> userIdList) {
        List<UserDetail> userByIdList = getUserByIdList(userIdList);
        return userByIdList.stream().collect(Collectors.toMap(UserDetail::getUserId, Function.identity(), (e1, e2) -> e1));
    }

    public List<UserDetail> getUserByIdList(List<String> userIdList) {
        Query userQuery = new Query(Criteria.where("_id").in(userIdList));
        List<User> all = repository.findAll(userQuery);

        return all.stream().map(this::getUserDetail).collect(Collectors.toList());
    }

    /**
     * 根据用户ID，加载用户新
     *
     * @param userId
     * @return
     */
    public UserDetail loadUserById(ObjectId userId) {
        Optional<User> userOptional = repository.findById(userId, new Field());
        if (userOptional.isPresent()) {
            User user = userOptional.get();
            return getUserDetail(user);
        }
        return null;
    }

    /**
     * 根据外部用户ID加载TM内部用户信息
     *
     * @param userId authing id
     * @return
     */
  /*  public UserDetail loadUserByExternalId(String userId) {

        Optional<User> userOptional = repository.findOne(Query.query(Criteria.where("externalUserId").is(userId).and("customId").is(userId)));
        if (userOptional.isPresent()) {
            User user = userOptional.get();
            return new UserDetail(user, Collections.singleton(new SimpleGrantedAuthority("USERS")));
        }
        return null;
    }*/

    /**
     * 根据外部用户ID加载TM内部用户信息
     * 调用TCM接口查询用户的信息,查询到就写入到 user 表,并返回 UserDetail 对象
     *
     * @param userId authing id
     * @return
     */
    public UserDetail loadUserByExternalId(String userId) {

        final Query userQuery = Query.query(Criteria.where("externalUserId").is(userId));
        Optional<User> userOptional = repository.findOne(userQuery);
        User user = null;
        UserDetail userDetail = null;
        if (userOptional.isPresent()) {
            user = userOptional.get();
//            return new UserDetail(user, Collections.singleton(new SimpleGrantedAuthority("USERS")));
            if (user.getLastUpdAt() == null ||
                    (System.currentTimeMillis() - user.getLastUpdAt().getTime()) > 1000 * 60 * 60) {
                UserInfoDto userInfoDto = tcmService.getUserInfo(userId);
                User tcmUser = buildUserFromTcmUser(userInfoDto, userId);

                tcmUser.setLastUpdAt(new Date());
                Update userUpdate = repository.buildUpdateSet(tcmUser);
                UpdateResult res = repository.getMongoOperations().upsert(userQuery, userUpdate, User.class);
                user = repository.findOne(userQuery).orElse(null);
            }
            userDetail = new UserDetail(user, Collections.singleton(new SimpleGrantedAuthority("USERS")));
        } else {
            UserInfoDto userInfoDto = tcmService.getUserInfo(userId);
            if (null != userInfoDto) {
                user = buildUserFromTcmUser(userInfoDto, userId);

                // 新增的用户，创建一个默认商户
                CustomerDto customerDto = customerService.createDefaultCustomer(user);
                user.setCustomId(customerDto.getId().toHexString());

                Notification notification = new Notification();
                notification.setConnected(new Connected(true, false, false));
                notification.setStoppedByError(new StoppedByError(true, false, false));
                notification.setConnectionInterrupted(new ConnectionInterrupted(true, false, false));
                user.setNotification(notification);

                final String email = user.getEmail();
                user.setEmail(null);
                Update userUpdate = repository.buildUpdateSet(user);
                //Query query = Query.query(Criteria.where("email").is(email));
                UpdateResult res = repository.getMongoOperations().upsert(userQuery, userUpdate, User.class);
                // 没有插入新记录时，删除新生成的 customer 信息
                if (res.getUpsertedId() == null) {
                    customerService.deleteById(customerDto.getId());
                }
                // 查询数据库中的用户信息，有可能是之前 insert 进去的
                Optional<User> optional = repository.findOne(userQuery);
                if (optional.isPresent())
                    user = optional.get();

                userDetail = getUserDetail(user);
                roleMappingService.initUserDefaultRole(user, userDetail);

                UserDetail finalUserDetail = userDetail;
                CommonUtils.ignoreAnyError(() -> {
                    //添加ldp目录
                    ldpService.addLdpDirectory(finalUserDetail);
                }, "TMUSER");
            }
        }

        return userDetail;
    }

    public User buildUserFromTcmUser(UserInfoDto userInfoDto, String externalUserId) {
        User user = BeanUtil.copyProperties(userInfoDto, User.class, "id");
        //属性名称不一样的，需要单独set进去
        user.setPhone(userInfoDto.getTelephone());

        //如果email为空，就生成一个userId+@custom.com后缀的邮箱。如  60bdf759aadcda0267e71a8b@custom.com
        if (StringUtils.isEmpty(user.getEmail())) {
            user.setEmail(user.getUserId() + DEFAULT_MAIL_SUFFIX);
        }

        user.setAccountStatus(userInfoDto.getUserStatus());

        //新增的用户，生成一个accessCode,生成一个64位的随机字符串
        user.setAccessCode(UUIDUtil.get64UUID());

        //set 外部用户id,对应tcm返回的userInfoDto  userId
        user.setExternalUserId(externalUserId);

        //当前操作这条记录的用户id
        user.setUserId(externalUserId);

        user.setLastUpdAt(new Date());
        user.setLastUpdBy(userInfoDto.getUsername());
        user.setCreateAt(new Date());
        user.setCreateUser(userInfoDto.getUsername());
        return user;
    }


    /**
     * @param id
     * @param {"notification":{"connected":{"email":true,"sms":false},"connectionInterrupted":{"email":true,"sms":true},"stoppedByError":{"email":true,"sms":true}}} {"guideData":{"noShow":false,"updateTime":1632380823831,"action":false}}
     *                                                                                                                                                               {\"guideData\":{\"noShow\":false,\"updateTime\":1632380823831,\"action\":false}}"
     * @return
     */
    public UserDto updateUserSetting(String id, String settingJson, UserDetail userDetail,Locale locale) {
        JSONObject jsonObject = new JSONObject(settingJson);
        Iterator<String> iterator = jsonObject.keySet().iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            Object value = jsonObject.get(key);
            if ("password".equals(key) && value != null) {
                if (StringUtils.isEmpty(value.toString())) {
                    continue;
                } else {
                    value = BCrypt.hashpw(value.toString());
                }
            }
            Update update = Update.update(key, value);
            update.set("last_updated", new Date());
            Query query = new Query(Criteria.where("id").is(id));
            UpdateResult updateResult = repository.getMongoOperations().updateMulti(query, update, User.class);
        }
        UserDto userDto = findById(toObjectId(id));
        List<RoleMappingDto> roleMappingDtos = updateRoleMapping(id, userDto.getRoleusers(), userDetail);
        userDto.setRoleMappings(roleMappingDtos);
        userLogService.addUserLog(Modular.USER, Operation.UPDATE, userDetail.getUserId(), userDto.getUserId(), StringUtils.isNotBlank(userDto.getLdapAccount()) ? userDto.getLdapAccount() : userDto.getEmail());
        return userDto;
    }

    /**
     * @param {"notification":{"connected":{"email":true,"sms":false},"connectionInterrupted":{"email":true,"sms":true},"stoppedByError":{"email":true,"sms":true}}}
     * @return
     */
  /*  public UpdateResult updateUserNotification(String id, String notificationStr) {
        JSONObject notification = JSONUtil.parseObj(notificationStr);
        Update update = Update.update("notification", notification);
        Query query = new Query(Criteria.where("id").is(id));
        UpdateResult wr = repository.getMongoOperations().updateMulti(query, update, User.class);
        return wr;
    }
*/
    public UpdateResult updateById(User user) {
        Update update = Update.update("notification", user.getNotification());
        Query query = new Query(Criteria.where("id").is(user.getId()));
        UpdateResult wr = repository.getMongoOperations().updateMulti(query, update, User.class);
        return wr;
    }

    public User findOneByEmail(String email) {
        Optional<User> userOptional = repository.findOne(Query.query(Criteria.where("email").is(email)
                .orOperator(Criteria.where("isDeleted").is(false), Criteria.where("isDeleted").exists(false))));
        if (!userOptional.isPresent()) {
            throw new BizException(checkLoginBriefTipsEnable("User.email.Found"));
        }
        return userOptional.get();
    }

    public <T extends BaseDto> UserDto save(CreateUserRequest request, UserDetail userDetail) {

        UserDto userDto = findOne(Query.query(Criteria.where("email").is(request.getEmail()).and("ldapAccount").is(request.getLdapAccount()).orOperator(Criteria.where("isDeleted").is(false), Criteria.where("isDeleted").exists(false))));
        if (userDto != null) {
            throw new BizException("User.Already.Exists");
        }

        if (StringUtils.isNotBlank(request.getSource()) && !"create".equals(request.getSource())) {
            List<RoleDto> roleDtos = roleService.findAll(Query.query(Criteria.where("register_user_default").is(true)));
            if (CollectionUtils.isNotEmpty(roleDtos)) {
                List<RoleMappingDto> roleMappingDtos = new ArrayList<>();
                roleDtos.forEach(roleDto -> {
                    RoleMappingDto roleMappingDto = new RoleMappingDto();
                    roleMappingDto.setPrincipalType("USER");
                    roleMappingDto.setPrincipalId(userDetail.getUserId());
                    roleMappingDto.setRoleId(roleDto.getId());
                    roleMappingDtos.add(roleMappingDto);
                });
                if (CollectionUtils.isNotEmpty(roleMappingDtos)) {
                    roleMappingService.save(roleMappingDtos, userDetail);
                }
            }
        }

        User user = new User();
        BeanUtils.copyProperties(request, user);
        if (StringUtils.isNotBlank(request.getPassword())) {
            user.setPassword(BCrypt.hashpw(request.getPassword()));
        }
        user.setAccessCode(randomHexString());

        //云版用户默认都有notification属性
        Notification notification = new Notification();
        user.setNotification(notification);
        User save = repository.save(user, userDetail);
        UserDto result = convertToDto(save, dtoClass);
        List<RoleMappingDto> roleMappingDtos = updateRoleMapping(save.getId().toHexString(), request.getRoleusers(), userDetail);
        result.setRoleMappings(roleMappingDtos);

        //添加ldp目录
        ldpService.addLdpDirectory(getUserDetail(user));
        userLogService.addUserLog(Modular.USER, Operation.CREATE, userDetail, save.getUserId(), StringUtils.isNotBlank(save.getLdapAccount()) ? save.getLdapAccount() : save.getEmail(), "createLdap".equals(save.getSource()));

        return result;
    }
    protected List<RoleMappingDto> updateRoleMapping(String userId, List<Object> roleusers, UserDetail userDetail) {
        // delete old role mapping
        if (CollectionUtils.isNotEmpty(roleusers)) {
            long deleted = roleMappingService.deleteAll(Query.query(Criteria.where("principalId").is(userId).and("principalType").is("USER")));
            log.info("delete old role mapping for userId {}, deleted: {}", userId, deleted);
        // add new role mapping
            List<RoleMappingDto> roleMappingDtos = roleusers.stream().map(r -> (String) r).map(roleId -> {
                RoleMappingDto roleMappingDto = new RoleMappingDto();
                roleMappingDto.setPrincipalType("USER");
                roleMappingDto.setPrincipalId(userId);
                roleMappingDto.setRoleId(new ObjectId(roleId));
                return roleMappingDto;
            }).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(roleMappingDtos)) {
                return roleMappingService.updateUserRoleMapping(roleMappingDtos, userDetail);
            }
        }
        return null;
    }

    protected String randomHexString() {
        return IntStream.range(0, 8).mapToObj(i -> Integer.toHexString(Double.valueOf((1 + Math.random()) * 0x10000).intValue()).substring(1)).collect(Collectors.joining());
    }

    public Long changePassword(ChangePasswordRequest request, UserDetail userDetail) {
        User user = findOneByEmail(userDetail.getEmail());
        if (BCrypt.checkpw(request.getOldPassword(), user.getPassword())) {
            UpdateResult updateResult = repository.update(Query.query(Criteria.where("_id").is(user.getId())), Update.update("password", BCrypt.hashpw(request.getNewPassword())));
            return updateResult.getModifiedCount();
        } else {
            throw new BizException("Incorrect.Password");
        }
    }

    public UserDto updatePhone(UserDetail loginUser, BindPhoneReq bindPhoneReq) {
        UpdateResult updateResult = repository.update(
                Query.query(Criteria.where("_id").is(loginUser.getUserId())),
                Update.update("phone", bindPhoneReq.getPhone())
                        .set("phoneVerified", bindPhoneReq.isPhoneVerified())
                        .set("phoneCountryCode", bindPhoneReq.getPhoneCountryCode())
        );

        userLogService.addUserLog(Modular.USER,
                bindPhoneReq.isBindPhone() ? Operation.BIND_PHONE : Operation.UPDATE_PHONE, loginUser, loginUser.getUsername());
        return findById(new ObjectId(loginUser.getUserId()));
    }

    public UserDto updateEmail(UserDetail loginUser, BindEmailReq bindEmailReq) {
        UpdateResult updateResult = repository.update(
                Query.query(Criteria.where("_id").is(loginUser.getUserId())),
                Update.update("email", bindEmailReq.getEmail())
                        .set("emailVerified", bindEmailReq.isEmailVerified())
        );
        userLogService.addUserLog(Modular.USER,
                bindEmailReq.isBindEmail() ? Operation.BIND_EMAIL : Operation.UPDATE_EMAIL, loginUser, loginUser.getUsername());
        return findById(new ObjectId(loginUser.getUserId()));
    }


    /**
     * 重置密码
     *
     * @return
     */
    public SendStatus sendValidateCde(String email) {
        SendStatus sendStatus = new SendStatus();
        User user = findOneByEmail(email);
        if (null != user) {
            String validateCode = RandomUtil.randomNumbers(6);
            sendStatus = mailUtils.sendValidateCodeForResetPWD(email, user.getUsername(), validateCode);
            if ("true".equals(sendStatus.getStatus())) {
                Date validateCodeSendTime = new Date();
                Update update = new Update();
                update.set("validateCode", validateCode);
                update.set("validateCodeSendTime", validateCodeSendTime);

                Query query = Query.query(Criteria.where("_id").is(user.getId()));
                repository.getMongoOperations().updateFirst(query, update, "User");
            } else {
                log.error("重置密码，邮件发送失败： " + sendStatus.getErrorMessage());
            }
        } else {
            sendStatus.setStatus("false");
            sendStatus.setErrorMessage("User not find: " + email);
        }
        return sendStatus;
    }

    /**
     * 重置密码 重置成功之后，验证码旧失效了。
     *
     * @param resetPasswordParam
     * @return
     */
    public Long reset(ResetPasswordParam resetPasswordParam) {
        String newPassword = resetPasswordParam.getNewPassword();
        String email = resetPasswordParam.getEmail();
        String validateCode = resetPasswordParam.getValidateCode();

        User user = repository.getMongoOperations().findOne(Query.query(Criteria.where("email").is(email)), User.class);
        if (null == user) {
            throw new BizException("User.Not.Exist");
        }
        if (!validateCode.equals(user.getValidateCode())) {
            log.info("验证码不正确");
            throw new BizException("ValidateCode.Not.Incorrect");
        }
        if (DateUtil.offsetMinute(user.getValidateCodeSendTime(), 5).toJdkDate().before(new Date())) {
            log.info("验证码 已超时");
            throw new BizException("ValidateCode.Is.expired");
        }

        UpdateResult updateResult = repository.update(Query.query(Criteria.where("_id").is(user.getId())), Update.update("password", BCrypt.hashpw(newPassword)));
        if (updateResult.getModifiedCount() == 1) {
            log.info("user:{}  已经成功重置密码", user.getEmail());
            Update update = Update.update("password", BCrypt.hashpw(newPassword));
            update.unset("validateCode");
            update.unset("validateCodeSendTime");
            repository.update(Query.query(Criteria.where("_id").is(user.getId())), update);

        }
        return updateResult.getModifiedCount();
    }


    /**
     * 逻辑删除
     *
     * @param id
     */
    public void delete(String id, UserDetail userDetail) {
        //delete role mapping
        roleMappingService.deleteAll(Query.query(Criteria.where("principalId").is(id).and("principalType").is("USER")));
        Update update = new Update().set("isDeleted", true);
        Query query = Query.query(Criteria.where("id").is(id));
        UpdateResult updateResult = repository.getMongoOperations().updateFirst(query, update, User.class);
        Field field = new Field();
        field.put("email", 1);
        field.put("ldapAccount", 1);
        UserDto user = findById(new ObjectId(id), field);
        if (updateResult.getModifiedCount() > 0) {
            userLogService.addUserLog(Modular.USER, Operation.DELETE, userDetail.getUserId(), id, StringUtils.isNotBlank(user.getLdapAccount()) ? user.getLdapAccount() : user.getEmail());
        }
    }

    public String getMongodbUri() {
        return mongodbUri;
    }

    public String getServerPort() {
        return serverPort;
    }

    public String isSsl() {
        return ssl;
    }

    public String getCaPath() {
        return caPath;
    }

    public String getKeyPath() {
        return keyPath;
    }
	public void updatePermissionRoleMapping(UpdatePermissionRoleMappingDto dto, UserDetail userDetail) {
		BiConsumer<List<RoleMappingDto>, Bi3Consumer<List<PermissionEntity>, List<RoleMappingDto>, ObjectId>> biConsumer = (roleMappingDtos, consumer) -> {
			if (CollectionUtils.isNotEmpty(roleMappingDtos)) {
				Map<ObjectId, List<RoleMappingDto>> roleMappingByRoleIdMap = roleMappingDtos.stream().collect(Collectors.groupingBy(RoleMappingDto::getRoleId));
				for (Map.Entry<ObjectId, List<RoleMappingDto>> entry : roleMappingByRoleIdMap.entrySet()) {
					List<RoleMappingDto> dtos = entry.getValue();
					ObjectId roleId = entry.getKey();
					//查询当前页面菜单的权限
					List<PermissionEntity> permissions = getPermissionsByCodes(dtos.stream().map(RoleMappingDto::getPrincipalId).filter(StringUtils::isNotEmpty).collect(Collectors.toSet()));
					//获取指定的页面菜单的权限及其子权限
					List<RoleMappingDto> wholeRoleMappingDto = getWholeRoleMappingDto(permissions.stream().map(PermissionEntity::getName).collect(Collectors.toSet()), roleId);
					consumer.accept(permissions, wholeRoleMappingDto, roleId);
				}
			}
		};

		biConsumer.accept(dto.getDeletes(), (permissions, deleteRoleMappingDtos, roleId) -> {
			List<Criteria> deleteCriteriaList = deleteRoleMappingDtos.stream()
							.map(delete -> Criteria.where("roleId").is(delete.getRoleId())
											.and("principalId").is(delete.getPrincipalId())
											.and("principalType").is(PrincipleType.PERMISSION))
							.collect(Collectors.toList());
			if (CollectionUtils.isNotEmpty(deleteCriteriaList)) {
				roleMappingService.deleteAll(Query.query(new Criteria().orOperator(deleteCriteriaList)));
			}
			//删除没有页面菜单的父权限
			List<RoleMappingDto> toDeleteParentRoleMappingDtos = getTodoParentRoleMappingDtos(roleId, permissions);
			if (CollectionUtils.isNotEmpty(toDeleteParentRoleMappingDtos)) {
				roleMappingService.deleteAll(Query.query(new Criteria().orOperator(toDeleteParentRoleMappingDtos.stream()
								.map(delete -> Criteria.where("roleId").is(delete.getRoleId())
												.and("principalId").is(delete.getPrincipalId())
												.and("principalType").is(PrincipleType.PERMISSION))
								.collect(Collectors.toList()))));
			}
		});

		biConsumer.accept(dto.getAdds(), (permissions, addRoleMappingDtos, roleId) -> {
			if (CollectionUtils.isNotEmpty(addRoleMappingDtos)) {
				//去重
				List<Criteria> queryExistsCriteriaList = addRoleMappingDtos.stream()
								.map(r -> Criteria.where("roleId").is(r.getRoleId())
												.and("principalId").is(r.getPrincipalId())
												.and("principalType").is(PrincipleType.PERMISSION))
								.collect(Collectors.toList());
				List<RoleMappingDto> alreadyExistsRoleMappings = roleMappingService.findAll(Query.query(new Criteria().orOperator(queryExistsCriteriaList)));
				if (CollectionUtils.isNotEmpty(alreadyExistsRoleMappings)) {
					Set<String> alreadyExistsPermissionCodes = alreadyExistsRoleMappings.stream().map(RoleMappingDto::getPrincipalId).collect(Collectors.toSet());
					addRoleMappingDtos = addRoleMappingDtos.stream().filter(r -> !alreadyExistsPermissionCodes.contains(r.getPrincipalId())).collect(Collectors.toList());
				}
				if (CollectionUtils.isNotEmpty(addRoleMappingDtos)) {
					roleMappingService.save(addRoleMappingDtos, userDetail);
				}
			}
			//添加没有的父权限
			Set<String> parentPermissionCodes = getParentPermissionCodes(permissions);
			if (CollectionUtils.isNotEmpty(parentPermissionCodes)) {
				List<Criteria> queryParentCriteriaList = parentPermissionCodes.stream()
								.map(code -> Criteria.where("roleId").is(roleId)
												.and("principalId").is(code)
												.and("principalType").is(PrincipleType.PERMISSION))
								.collect(Collectors.toList());
				List<RoleMappingDto> alreadyExistsParentRoleMappings = roleMappingService.findAll(Query.query(new Criteria().orOperator(queryParentCriteriaList)));
				alreadyExistsParentRoleMappings.stream().map(RoleMappingDto::getPrincipalId).forEach(parentPermissionCodes::remove);
				List<RoleMappingDto> toAddParentRoleMappingDtos = parentPermissionCodes.stream()
								.map(code -> new RoleMappingDto(PrincipleType.PERMISSION.getValue(), code, roleId)).collect(Collectors.toList());
				if (CollectionUtils.isNotEmpty(toAddParentRoleMappingDtos)) {
					roleMappingService.save(toAddParentRoleMappingDtos, userDetail);
				}
			}
		});
        if (dto.getAdds().size() > 0) {
            roleMappingService.addUserLogIfNeed(dto.getAdds(), userDetail);
        } else if (dto.getDeletes().size() > 0) {
            roleMappingService.addUserLogIfNeed(dto.getDeletes(), userDetail);
        }
    }
    @NotNull
    private List<RoleMappingDto> getTodoParentRoleMappingDtos(ObjectId roleId, List<PermissionEntity> permissions) {
        List<RoleMappingDto> todoParentRoleMappingDtos = new ArrayList<>();
        Set<String> parentPermissionCodes = getParentPermissionCodes(permissions);
        for (String parentPermissionCode : parentPermissionCodes) {
            List<PermissionEntity> allChildPermissions = getChildPermissionsByCodes(Collections.singleton(parentPermissionCode));
            List<Criteria> queryChildCriteriaList = allChildPermissions.stream().map(p -> Criteria.where("roleId").is(roleId)
                            .and("principalId").is(p.getName())
                            .and("principalType").is(PrincipleType.PERMISSION))
                    .collect(Collectors.toList());
            long count = roleMappingService.count(Query.query(new Criteria().orOperator(queryChildCriteriaList)));
            if (count <= 0) {
                // need to do something
                todoParentRoleMappingDtos.add(new RoleMappingDto(PrincipleType.PERMISSION.getValue(), parentPermissionCode, roleId));
            }
        }
        return todoParentRoleMappingDtos;
    }

    private List<RoleMappingDto> getWholeRoleMappingDto(Set<String> permissionCodes, ObjectId roleId) {
        List<RoleMappingDto> roleMappingDtos = permissionCodes.stream().map(p -> new RoleMappingDto(PrincipleType.PERMISSION.getValue(), p, roleId)).collect(Collectors.toList());
        List<RoleMappingDto> childRoleMappingDto = getChildRoleMappingDto(permissionCodes, roleId);
        if (CollectionUtils.isNotEmpty(childRoleMappingDto)) {
            roleMappingDtos.addAll(childRoleMappingDto);
        }
        return roleMappingDtos;
    }

    private List<RoleMappingDto> getChildRoleMappingDto(Set<String> permissionCodes, ObjectId roleId) {
        List<PermissionEntity> childPermissions = getChildPermissionsByCodes(permissionCodes);
        if (CollectionUtils.isNotEmpty(childPermissions)) {
            return childPermissions.stream()
                    .map(p -> new RoleMappingDto(PrincipleType.PERMISSION.getValue(), p.getName(), roleId)).collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    private Set<String> getParentPermissionCodes(List<PermissionEntity> permissions) {
        return permissions.stream().map(PermissionEntity::getParentId).filter(StringUtils::isNotEmpty).collect(Collectors.toSet());
    }

    private List<PermissionEntity> getChildPermissionsByCodes(Set<String> permissionCodes) {
        return permissionService.find(new Filter(Where.where("parentId", new HashMap<String, Object>() {{
            put("$in", permissionCodes);
        }})));
    }

	/**
	 * {"$and":
	 *     [
	 *         {name:"v2_datasource_menu"},
	 *         {"$and":
	 *             [
	 *                 {parentId:{"$ne": null}},
	 *                 {parentId:{"$ne":""}},
	 *                 {parentId:{"$exists":true}}
	 *             ]
	 *         }
	 *      ]
	 * }
	 * @param permissionCodes
	 * @return
	 */
    private List<PermissionEntity> getPermissionsByCodes(Set<String> permissionCodes) {

			Set<String> topCodes = topPermissionCodes.stream().filter(permissionCodes::contains).collect(Collectors.toSet());
			Where where = Where.where("$or", new ArrayList<Map<String, Object>>() {{
				add(new HashMap<String, Object>() {{
					put("name", new HashMap<String, Object>() {{
						put("$in", topCodes);
					}});
				}});
				add(new HashMap<String, Object>() {{
					put("$and", new ArrayList<Map<String, Object>>() {{
						add(new HashMap<String, Object>() {{
							put("name", new HashMap<String, Object>() {{
								put("$in", permissionCodes);
							}});
						}});
						add(new HashMap<String, Object>() {{
							put("$and", new ArrayList<Map<String, Object>>() {{
								add(new HashMap<String, Object>() {{
									put("parentId", new HashMap<String, Object>() {{
										put("$ne", null);
									}});
								}});
								add(new HashMap<String, Object>() {{
									put("parentId", new HashMap<String, Object>() {{
										put("$ne", "");
									}});
								}});
								add(new HashMap<String, Object>() {{
									put("parentId", new HashMap<String, Object>() {{
										put("$exists", true);
									}});
								}});
							}});
						}});
					}});
				}});
			}});

//			List<PermissionEntity> resultList = new ArrayList<>(pagePermissionEntities);
			//获取所有的parentId的值
//			List<PermissionEntity> topPermissionAndNoChild = permissionService.getTopPermissionAndNoChild(permissionCodes);
//			if (CollectionUtils.isNotEmpty(topPermissionAndNoChild)) {
//				resultList.addAll(topPermissionAndNoChild);
//			}

			return permissionService.find(new Filter(where));
    }

		private final static Set<String> topPermissionCodes = new HashSet<String>() {{
			add("v2_dashboard");
			add("v2_data-console");
			add("v2_datasource_menu");
			add("v2_data_pipeline");
			add("v2_advanced_features");
			add("v2_data_replication");
			add("v2_data_flow");
			add("v2_data_check");
			add("v2_log_collector");
			add("v2_function_management");
			add("v2_custom_node");
			add("v2_shared_cache");
			add("v2_data_discovery");
			add("v2_data_object");
			add("v2_data_catalogue");
			add("v2_data");
			add("v2_data-server");
			add("v2_api-application");
			add("v2_data-server-list");
			add("v2_api-client");
			add("v2_api-servers");
			add("v2_data_server_audit");
			add("v2_api_monitor");
			add("v2_system-management");
			add("v2_external-storage_menu");
			add("v2_cluster-management_menu");
			add("v2_user_management_menu");
			add("v2_role_management");
		}};

    @Override
    public boolean checkLdapLoginEnable() {
        Settings settings = settingsService.getByCategoryAndKey(CategoryEnum.LDAP, KeyEnum.LDAP_LOGIN_ENABLE);
        if (settings != null) {
            return settings.getOpen();
        }
        return false;
    }

    @Override
    public boolean checkLoginSingleSessionEnable() {
        Settings settings = settingsService.getByCategoryAndKey(CategoryEnum.LOGIN, KeyEnum.LOGIN_SINGLE_SESSION);
        if (settings != null) {
            return settings.getOpen();
        }
        return false;
    }

    @Override
    public String checkLoginBriefTipsEnable(String messageCode) {
        Settings settings = settingsService.getByCategoryAndKey(CategoryEnum.LOGIN, KeyEnum.LOGIN_BRIEF_TIPS);
        if (settings != null && settings.getOpen()) {
            return "Incorrect.Vague";
        }
        return messageCode;
    }

    @Override
    public TestResponseDto testLoginByLdap(TestLdapDto testldapDto) {
        String ldapUrl = testldapDto.getLdap_Server_Host() + ":" + testldapDto.getLdap_Server_Port();
        String bindDN = testldapDto.getLdap_Bind_DN();
        String bindPassword = testldapDto.getLdap_Bind_Password();
        Boolean sslEnable = testldapDto.getLdap_SSL_Enable();
        String ldapSslCert = testldapDto.getLdap_SSL_Cert();
        LdapLoginDto ldapLoginDto = LdapLoginDto.builder().ldapUrl(ldapUrl).bindDN(bindDN).password(bindPassword).sslEnable(sslEnable).cert(ldapSslCert).build();
        if ("*****".equals(ldapLoginDto.getPassword())) {
            String value = SettingsEnum.LDAP_PASSWORD.getValue();
            ldapLoginDto.setPassword(value);
        }
        DirContext dirContext = null;
        try {
            dirContext = buildDirContext(ldapLoginDto);
            if (null != dirContext) {
                return new TestResponseDto(true, null);
            } else {
                return new TestResponseDto(false, "connect to active directory server failed");
            }
        } catch (Exception e) {
            return new TestResponseDto(false, TapSimplify.getStackTrace(e));
        } finally {
            close(dirContext);
        }
    }

    @Override
    public boolean loginByLdap(String username, String password) {
        List<Settings> all = settingsService.findAll();
        Map<String, Object> collect = all.stream().collect(Collectors.toMap(Settings::getKey, Settings::getValue, (e1, e2) -> e1));

        String host = (String) collect.get("ldap.server.host");
        String port = (String) collect.get("ldap.server.port");
        String bindDN = (String) collect.get("ldap.bind.dn");
        String pwd = (String) collect.get("ldap.bind.password");
        String baseDN = (String) collect.get("ldap.base.dn");
        String cert = (String) collect.get("ldap.ssl.cert");
        Boolean ssl = false;
        Settings settings = settingsService.getByCategoryAndKey(CategoryEnum.LDAP, KeyEnum.LDAP_SSL_ENABLE);
        if (settings != null) {
            ssl = settings.getOpen();
        }
        String ldapUrl = host + ":" + port;
        LdapLoginDto ldapLoginDto = LdapLoginDto.builder().ldapUrl(ldapUrl).bindDN(bindDN).password(pwd).baseDN(baseDN).sslEnable(ssl).cert(cert).build();
        DirContext dirContext = null;
        try {
            boolean exists = searchUser(ldapLoginDto, username);
            if (!exists) {
                throw new BizException("AD.Account.Not.Exists");
            }
            ldapLoginDto.setBindDN(username);
            ldapLoginDto.setPassword(password);
            dirContext = buildDirContext(ldapLoginDto);
            if (null != dirContext) {
                return true;
            }
        } finally {
            close(dirContext);
        }
        return false;
    }

    private void close(DirContext dirContext) {
        if (null != dirContext) {
            try {
                dirContext.close();
            } catch (NamingException e) {
                // do nothing
            }
        }
    }

    @Override
    public UserDto getUserDetail(String userId) {
        UserDto userDto = findById(toObjectId(userId));
        userDto.setCreateTime(userDto.getCreateAt());
        List<RoleMappingDto> roleMappingDtoList = roleMappingService.getUser(PrincipleType.USER, userId);
        if (CollectionUtils.isNotEmpty(roleMappingDtoList)) {
            List<ObjectId> objectIds = roleMappingDtoList.stream().map(RoleMappingDto::getRoleId).collect(Collectors.toList());
            List<RoleDto> roleDtos = roleService.findAll(Query.query(Criteria.where("_id").in(objectIds)));
            if (CollectionUtils.isNotEmpty(roleDtos)) {
                roleDtos.forEach(roleDto -> roleMappingDtoList.stream()
                        .filter(roleMappingDto -> roleDto.getId().toHexString().equals(roleMappingDto.getRoleId().toHexString()))
                        .findFirst().ifPresent(roleMappingDto -> roleMappingDto.setRole(roleDto)));
            }
            userDto.setRoleMappings(roleMappingDtoList);
        }
        userDto.setPermissions(permissionService.getCurrentPermission(userId));
        return userDto;
    }

    protected boolean searchUser(LdapLoginDto ldapLoginDto, String username) {
        String sAMAccountNameFilter = String.format("(sAMAccountName=%s)", username);
        String userPrincipalNameFilter = String.format("(userPrincipalName=%s)", username);
        DirContext ctx = buildDirContext(ldapLoginDto);
        String searchBases = ldapLoginDto.getBaseDN();
        if (StringUtils.isBlank(searchBases)) return false;
        try {
            SearchControls searchControls = new SearchControls();
            searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            searchControls.setReturningAttributes(new String[]{"sAMAccountName", "userPrincipalName", "displayName"});
            String[] searchBase = searchBases.split(";");
            for (String base : searchBase) {
                boolean isExist = searchWithFilter(ctx, base, sAMAccountNameFilter, searchControls) || searchWithFilter(ctx, base, userPrincipalNameFilter, searchControls);
                if (isExist) {
                    ldapLoginDto.setBaseDN(base);
                    return true;
                }
            }
            return false;
        } catch (NamingException e) {
            throw new BizException("AD.Search.Fail", e);
        } finally {
            close(ctx);
        }
    }

    protected boolean searchWithFilter(DirContext ctx, String searchBase, String filter, SearchControls searchControls) throws NamingException {
        NamingEnumeration<SearchResult> sAMAccountNameResult = ctx.search(searchBase, filter, searchControls);

        String userPrincipalName = null;
        String displayName = null;
        while (sAMAccountNameResult.hasMore()) {
            SearchResult searchResult = sAMAccountNameResult.next();
            Attributes attributes = searchResult.getAttributes();
            userPrincipalName = attributes.get("userPrincipalName") != null ? attributes.get("userPrincipalName").get().toString() : null;
            displayName = attributes.get("displayName") != null ? attributes.get("displayName").get().toString() : null;
        }
        return StringUtils.isNotBlank(userPrincipalName) || StringUtils.isNotBlank(displayName);
    }

    protected DirContext buildDirContext(LdapLoginDto ldapLoginDto) {
        try {
            String ldapUrl = ldapLoginDto.getLdapUrl();
            String bindDn = ldapLoginDto.getBindDN();
            String baseDN = ldapLoginDto.getBaseDN();
            String password = ldapLoginDto.getPassword();
            Boolean ssl = ldapLoginDto.isSslEnable();
            String certFile = ldapLoginDto.getCert();
            Hashtable<String, String> env = new Hashtable<>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            env.put(Context.PROVIDER_URL, ldapUrl);
            env.put(Context.SECURITY_AUTHENTICATION, "simple");
            if (!bindDn.contains("@") && StringUtils.isNotBlank(baseDN)) {
                String domain = convertBaseDnToDomain(baseDN);
                bindDn = bindDn + "@" + domain;
            }
            env.put(Context.SECURITY_PRINCIPAL, bindDn);
            env.put(Context.SECURITY_CREDENTIALS, password);
            if (ssl) {
                if (null == certFile) throw new BizException("AD.Login.Fail");
                try (InputStream certificates = new ByteArrayInputStream(certFile.getBytes())) {
                    SSLContext sslContext = createSSLContext(certificates);
                    HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
                }
            }
            DirContext ctx = new InitialDirContext(env);
            return ctx;
        } catch (NamingException e) {
            if (e.getMessage().toLowerCase().contains("error code 49")) {
                throw new BizException("AD.Login.WrongPassword", e);
            } else if (e.getMessage().toLowerCase().contains("tls handshake")) {
                throw new BizException("AD.Login.InvalidCert", e);
            } else if (e.getMessage().toLowerCase().contains("no subject alternative dns")) {
                throw new BizException("AD.Login.Retryable", e);
            } else {
                throw new BizException("AD.Login.Fail", e);
            }
        } catch (NullPointerException e) {
            throw new RuntimeException("please check ldap configuration, such as bind dn or password");
        } catch (Exception e) {
            throw new BizException("AD.Login.Fail", e);
        }
    }

    protected String convertBaseDnToDomain(String baseDn) {
        String[] parts = baseDn.split(",");
        StringBuilder domain = new StringBuilder();
        for (String part : parts) {
            part = part.trim();
            if (part.startsWith("DC=") || part.startsWith("dc=")) {
                if (domain.length() > 0) {
                    domain.append(".");
                }
                domain.append(part.substring(3));
            }
        }
        return domain.length() > 0 ? domain.toString() : null;
    }

    protected SSLContext createSSLContext(InputStream certFile) throws Exception {
        // load custom cert
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        X509Certificate caCert = (X509Certificate) cf.generateCertificate(certFile);
        certFile.close();

        // create KeyStore and import cert
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        // init empty keyStore
        keyStore.load(null, null);
        keyStore.setCertificateEntry("caCert", caCert);

        // create TrustManagerFactory and init KeyStore
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keyStore);

        TrustManager[] trustManagers = tmf.getTrustManagers();
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagers, null);

        return sslContext;
    }

    @Override
    public String refreshAccessCode(UserDetail userDetail) {
        String accessCode = randomHexString();
        if (StringUtils.isBlank(accessCode)) {
            throw new BizException("AccessCode.Is.Null");
        }
        Query query = Query.query(Criteria.where("_id").is(userDetail.getUserId()));
        UpdateResult updateResult = update(query, Update.update("accessCode", accessCode));
        Field field = new Field();
        field.put("accesscode", 1);
        UserDto userDto = findById(new ObjectId(userDetail.getUserId()), field);
        if (updateResult.getModifiedCount() > 0) {
            userLogService.addUserLog(Modular.ACCESS_CODE, Operation.UPDATE, userDetail.getUserId(), userDto.getUserId(), userDto.getAccessCode());
        }
        return userDto.getAccessCode();
    }
}
