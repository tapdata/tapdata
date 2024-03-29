package com.tapdata.tm.user.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.tcm.dto.UserInfoDto;
import com.tapdata.tm.user.dto.*;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.user.param.ResetPasswordParam;
import com.tapdata.tm.user.repository.UserRepository;
import lombok.NonNull;
import org.bson.types.ObjectId;
import org.springframework.security.core.userdetails.UserDetailsService;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public abstract class UserService extends BaseService<UserDto, User, ObjectId, UserRepository> implements UserDetailsService {
    public UserService(@NonNull UserRepository repository) {
        super(repository, UserDto.class, User.class);
    }
    public abstract UserDetail loadUserByUsername(String username);

    public abstract List<UserDetail> loadAllUser();

    public abstract Map<String, UserDetail> getUserMapByIdList(List<String> userIdList);

    public abstract List<UserDetail> getUserByIdList(List<String> userIdList);

    public abstract UserDetail loadUserById(ObjectId userId);

    public abstract UserDetail loadUserByExternalId(String userId);

    public abstract User buildUserFromTcmUser(UserInfoDto userInfoDto, String externalUserId);

    public abstract UserDto updateUserSetting(String id, String settingJson, UserDetail userDetail, Locale locale);

    public abstract UpdateResult updateById(User user);

    public abstract User findOneByEmail(String email);

    public abstract <T extends BaseDto> UserDto save(CreateUserRequest request, UserDetail userDetail);

    public abstract Long changePassword(ChangePasswordRequest request, UserDetail userDetail);

    public abstract UserDto updatePhone(UserDetail loginUser, BindPhoneReq bindPhoneReq);

    public abstract UserDto updateEmail(UserDetail loginUser, BindEmailReq bindEmailReq);

    public abstract Boolean sendValidateCde(String email);

    public abstract Long reset(ResetPasswordParam resetPasswordParam);

    public abstract void delete(String id);

    public abstract String getMongodbUri();

    public abstract String getServerPort();

    public abstract void updatePermissionRoleMapping(UpdatePermissionRoleMappingDto dto, UserDetail userDetail);
}

