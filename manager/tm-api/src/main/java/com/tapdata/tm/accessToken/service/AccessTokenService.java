package com.tapdata.tm.accessToken.service;

import com.tapdata.tm.accessToken.dto.AccessTokenDto;
import com.tapdata.tm.accessToken.entity.AccessTokenEntity;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.dto.GenerateAccessTokenDto;
import com.tapdata.tm.user.entity.User;
import org.bson.types.ObjectId;

public interface AccessTokenService {
    AccessTokenDto generateToken(GenerateAccessTokenDto generateAccessTokenDto);

    AccessTokenDto generateToken(String accessCode);

    void save(AccessTokenEntity accessTokenEntity);

    ObjectId validate(String accessToken);

    /**
     * 校验 token，并通过 {@code countAsActivity} 标记本次请求是否计入用户活跃。
     * <p>由 {@link com.tapdata.tm.base.security.LoginUserResolver} 根据 {@code X-User-Activity} 请求头判定：
     * 前端定时轮询等被动请求应传 {@code false}，避免无操作期间持续顺延滑动过期时间。
     * 默认值（无 header 时）为 {@code true}，向后兼容老前端。
     */
    ObjectId validate(String accessToken, boolean countAsActivity);

    AccessTokenDto save(User user);

    long removeAccessToken(String accessToken, UserDetail userDetail);

    long removeAccessTokenByAuthType(ObjectId userId, String authType);
}
