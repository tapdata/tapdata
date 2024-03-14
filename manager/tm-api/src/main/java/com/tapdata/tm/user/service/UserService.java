package com.tapdata.tm.user.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.base.service.IBaseService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.tcm.dto.UserInfoDto;
import com.tapdata.tm.user.dto.*;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.user.param.ResetPasswordParam;
import com.tapdata.tm.user.repository.UserRepository;
import org.bson.types.ObjectId;
import org.springframework.security.core.userdetails.UserDetailsService;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public interface UserService extends IBaseService<UserDto, User, ObjectId, UserRepository>,UserDetailsService {
    UserDetail loadUserByUsername(String username);

    List<UserDetail> loadAllUser();

    Map<String, UserDetail> getUserMapByIdList(List<String> userIdList);

    List<UserDetail> getUserByIdList(List<String> userIdList);

    UserDetail loadUserById(ObjectId userId);

    UserDetail loadUserByExternalId(String userId);

    User buildUserFromTcmUser(UserInfoDto userInfoDto, String externalUserId);

    UserDto updateUserSetting(String id, String settingJson, UserDetail userDetail, Locale locale);

    UpdateResult updateById(User user);

    User findOneByEmail(String email);

    <T extends BaseDto> UserDto save(CreateUserRequest request, UserDetail userDetail);

    Long changePassword(ChangePasswordRequest request, UserDetail userDetail);

    UserDto updatePhone(UserDetail loginUser, BindPhoneReq bindPhoneReq);

    UserDto updateEmail(UserDetail loginUser, BindEmailReq bindEmailReq);

    Boolean sendValidateCde(String email);

    Long reset(ResetPasswordParam resetPasswordParam);

    void delete(String id);

    String getMongodbUri();

    String getServerPort();

    void updatePermissionRoleMapping(UpdatePermissionRoleMappingDto dto, UserDetail userDetail);
}

