package com.tapdata.tm.user.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.crypto.digest.BCrypt;
import cn.hutool.json.JSONObject;
import com.google.common.collect.Sets;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.customer.dto.CustomerDto;
import com.tapdata.tm.customer.service.CustomerService;
import com.tapdata.tm.role.dto.RoleDto;
import com.tapdata.tm.role.service.RoleService;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import com.tapdata.tm.tcm.dto.UserInfoDto;
import com.tapdata.tm.tcm.service.TcmService;
import com.tapdata.tm.user.dto.*;
import com.tapdata.tm.user.entity.Connected;
import com.tapdata.tm.user.entity.ConnectionInterrupted;
import com.tapdata.tm.user.entity.Notification;
import com.tapdata.tm.user.entity.StoppedByError;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.user.param.ResetPasswordParam;
import com.tapdata.tm.user.repository.UserRepository;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.MailUtils;
import com.tapdata.tm.utils.SendStatus;
import com.tapdata.tm.utils.UUIDUtil;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.stereotype.Component;

import java.util.*;
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
public class UserService extends BaseService<UserDto, User, ObjectId, UserRepository> implements UserDetailsService {
    public UserService(@NonNull UserRepository repository) {
        super(repository, UserDto.class, User.class);
    }
    @Value("${spring.data.mongodb.uri}")
    private String mongodbUri;
    @Value("${server.port}")
    private String serverPort;
    @Autowired
    TcmService tcmService;

    @Autowired
    RoleMappingService roleMappingService;

    @Autowired
    private RoleService roleService;

    @Autowired
    private UserLogService userLogService;

    @Autowired
    private CustomerService customerService;

    private final String DEFAULT_MAIL_SUFFIX = "@custom.com";

    @Autowired
    MailUtils mailUtils;

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

    /**
     * 暂时这么用，等用户角色权限需求完善了 再调整
     *
     * @param user
     * @return
     */
    @NotNull
    private UserDetail getUserDetail(User user) {
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
    public UserDto updateUserSetting(String id, String settingJson) {
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
            Query query = new Query(Criteria.where("id").is(id));
            UpdateResult updateResult = repository.getMongoOperations().updateMulti(query, update, User.class);
        }
        UserDto userDto = findById(toObjectId(id));
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
        Optional<User> userOptional = repository.findOne(Query.query(Criteria.where("email").is(email)));
        if (!userOptional.isPresent()) {
            throw new BizException("User.email.Found");
        }
        return userOptional.get();
    }

    public <T extends BaseDto> UserDto save(CreateUserRequest request, UserDetail userDetail) {

        UserDto userDto = findOne(Query.query(Criteria.where("email").is(request.getEmail())));
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
        return convertToDto(save, dtoClass);
    }

    private String randomHexString() {
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
                        .set("areaCode", bindPhoneReq.getAreaCode())
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
    public Boolean sendValidateCde(String email) {
        Boolean sendResult = false;
        User user = findOneByEmail(email);
        if (null != user) {
            String validateCode = RandomUtil.randomNumbers(6);
            SendStatus sendStatus = mailUtils.sendValidateCode(email, user.getUsername(), validateCode);
            if ("true".equals(sendStatus.getStatus())) {
                Date validateCodeSendTime = new Date();
                Update update = new Update();
                update.set("validateCode", validateCode);
                update.set("validateCodeSendTime", validateCodeSendTime);

                Query query = Query.query(Criteria.where("_id").is(user.getId()));
                repository.getMongoOperations().updateFirst(query, update, "user");
                sendResult = true;
            } else {
                log.error("重置密码，邮件发送失败： msg", sendStatus.getErrorMessage());
            }
        }
        return sendResult;
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
    public void delete(String id) {
        Update update = new Update().set("isDeleted", true);
        Query query = Query.query(Criteria.where("id").is(id));
        UpdateResult updateResult = repository.getMongoOperations().updateFirst(query, update, User.class);
    }

    public String getMongodbUri() {
        return mongodbUri;
    }

    public String getServerPort() {
        return serverPort;
    }
}
