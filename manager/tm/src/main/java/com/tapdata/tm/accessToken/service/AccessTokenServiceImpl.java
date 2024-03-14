package com.tapdata.tm.accessToken.service;

import cn.hutool.core.bean.BeanUtil;
import com.mongodb.client.result.DeleteResult;
import com.tapdata.tm.accessToken.dto.AccessTokenDto;
import com.tapdata.tm.accessToken.dto.AuthType;
import com.tapdata.tm.accessToken.entity.AccessTokenEntity;
import com.tapdata.tm.accessToken.repository.AccessTokenRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.dto.GenerateAccessTokenDto;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.user.service.UserServiceImpl;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.UUIDUtil;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.Date;

@Service
@Slf4j
public class AccessTokenServiceImpl implements AccessTokenService {

    @Value("${access.token.ttl}")
    private Long accessTokenTtl;

    @Autowired
    AccessTokenRepository accessTokenRepository;

    @Autowired
    UserServiceImpl userService;

    @Autowired
    UserLogService userLogService;

    /**
     * 根据前端传进来的accessCode 先到user表里面找，
     * 如果找到，就认为登录成功，生成64位的  accessToken,并更新到accessToken表，，返回前端
     * 如果没找到，就任务登录失败，返回401
     *
     * @param generateAccessTokenDto
     * @return
     */
    public AccessTokenDto generateToken(GenerateAccessTokenDto generateAccessTokenDto) {
        AccessTokenDto accessTokenDto = null;
        String accessCode = generateAccessTokenDto.getAccesscode();

        Assert.notNull(accessCode, "");

        Query query = Query.query(Criteria.where("accesscode").is(accessCode));

        UserDto userDto = userService.findOne(query);
        if (null != userDto) {
            String id = UUIDUtil.get64UUID();
            Date now = new Date();
            AccessTokenEntity accessTokenEntity = new AccessTokenEntity(id, accessTokenTtl, now, userDto.getId(), now, AuthType.ACCESS_CODE.getValue());
            accessTokenDto = new AccessTokenDto();
            BeanUtil.copyProperties(accessTokenEntity, accessTokenDto);


            save(accessTokenEntity);
        }
        return accessTokenDto;
    }

    public AccessTokenDto generateToken(String accessCode) {
        AccessTokenDto accessTokenDto = null;
        Assert.notNull(accessCode, "");

        Query query = Query.query(Criteria.where("accesscode").is(accessCode));

        UserDto userDto = userService.findOne(query);
        if (null != userDto) {
            String id = UUIDUtil.get64UUID();
            Date now = new Date();
            AccessTokenEntity accessTokenEntity = new AccessTokenEntity(id, accessTokenTtl, now, userDto.getId(), now, AuthType.ACCESS_CODE.getValue());
            accessTokenDto = new AccessTokenDto();
            BeanUtil.copyProperties(accessTokenEntity, accessTokenDto);


            save(accessTokenEntity);
        }

        return accessTokenDto;
    }


    public void save(AccessTokenEntity accessTokenEntity) {
        accessTokenRepository.getMongoOperations().save(accessTokenEntity);
    }

    /**
     * 验证token是否有效
     *
     * @param accessToken
     * @return 有效返回用户ID，无效返回 null
     */
    public ObjectId validate(String accessToken) {
        AccessTokenEntity accessTokenEntity = accessTokenRepository.getMongoOperations().findById(accessToken, AccessTokenEntity.class);
        if (accessTokenEntity == null)
            return null;
        long expiredAt = accessTokenEntity.getCreated().getTime() + accessTokenEntity.getTtl() * 1000;
        if (new Date().getTime() <= expiredAt) {
            return accessTokenEntity.getUserId();
        }
        return null;
    }

    public AccessTokenDto save(User user) {
        AccessTokenDto accessTokenDto = null;
        if (user != null) {
            String id = UUIDUtil.get64UUID();
            Date now = new Date();
            AccessTokenEntity accessTokenEntity = new AccessTokenEntity(id, accessTokenTtl, now, user.getId(), now, AuthType.ACCESS_CODE.getValue());
            accessTokenDto = new AccessTokenDto();
            BeanUtil.copyProperties(accessTokenEntity, accessTokenDto);


            save(accessTokenEntity);
        }
        return accessTokenDto;
    }

    /**
     * 用户推出登录
     *
     * @param accessToken
     * @return
     */
    public long removeAccessToken(String accessToken, UserDetail userDetail) {
        DeleteResult result = accessTokenRepository.getMongoOperations().remove(Query.query(Criteria.where("_id").is(accessToken)), AccessTokenEntity.class);
        if (result.getDeletedCount() > 0) {
            //增加操作日志
            try {
                userLogService.addUserLog(Modular.SYSTEM, Operation.LOGOUT, userDetail, "");
            } catch (Exception e) {
                log.error("推出登录添加操作日志异常", e);
            }
        }

        return result.getDeletedCount();
    }
}
