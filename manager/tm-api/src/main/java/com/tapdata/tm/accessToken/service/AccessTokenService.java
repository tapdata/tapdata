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

    AccessTokenDto save(User user);

    long removeAccessToken(String accessToken, UserDetail userDetail);
}
