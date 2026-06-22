package com.tapdata.tm.accessToken.service;

import cn.hutool.core.bean.BeanUtil;
import com.mongodb.client.result.DeleteResult;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.service.SettingsService;
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
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
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

    @Autowired
    SettingsService settingsService;

    /** 解析后的 token 不活跃超时（秒），来自 Settings；带短期缓存，避免每次校验都查库 */
    private static final long TTL_CACHE_MS = 30_000L;
    private volatile long cachedTtlSeconds = -1L;
    private volatile long cachedTtlAt = 0L;

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
        return validate(accessToken, true);
    }

    /**
     * 与 {@link #validate(String)} 行为一致，但通过 {@code countAsActivity} 控制是否将本次请求计入用户活跃。
     * 当为 {@code false} 时，跳过 {@code last_updated} 的节流刷新，不顺延滑动过期时间——
     * 用于前端被动轮询（带 {@code X-User-Activity: 0}），避免用户实际无操作期间会话被持续延期。
     * 对 {@link AuthType#ACCESS_CODE} 类 token，绝对寿命不受该参数影响，仅影响 Mongo TTL 索引的顺延。
     */
    public ObjectId validate(String accessToken, boolean countAsActivity) {
        AccessTokenEntity accessTokenEntity = accessTokenRepository.getMongoOperations().findById(accessToken, AccessTokenEntity.class);
        if (accessTokenEntity == null)
            return null;

        long now = System.currentTimeMillis();
        boolean isAccessCodeAuthType = AuthType.ACCESS_CODE.getValue().equals(accessTokenEntity.getAuthType());

        long ttlSeconds;
        if (isAccessCodeAuthType) {
            ttlSeconds = accessTokenTtl != null ? accessTokenTtl : 0L;
        } else {
            ttlSeconds = resolveTtlSeconds();
        }
        if (ttlSeconds <= 0) {
            return null;
        }

        Date expiryBase;
        if (isAccessCodeAuthType) {
            expiryBase = accessTokenEntity.getCreated();
        } else{
            expiryBase = accessTokenEntity.getLastUpdated() != null ? accessTokenEntity.getLastUpdated() : accessTokenEntity.getCreated();
        }
        if (expiryBase == null) {
            return null;
        }
        if (now > expiryBase.getTime() + ttlSeconds * 1000) {
            return null;
        }

        if (countAsActivity) {
            Date refreshBase = accessTokenEntity.getLastUpdated() != null
                    ? accessTokenEntity.getLastUpdated()
                    : accessTokenEntity.getCreated();
            refreshLastUpdatedIfNeeded(accessToken, refreshBase, ttlSeconds, now);
        }
        return accessTokenEntity.getUserId();
    }

    /**
     * 获取浏览器登录态的不活跃过期时间（秒）：优先读取 Settings（以分钟为单位存储，此处换算为秒，
     * 支持运行时热加载），回退到 application.yml 中 {@code access.token.ttl}（秒）。
     * 带 {@value #TTL_CACHE_MS} 毫秒短期缓存，避免每次校验都查库。
     * 仅供 {@link AuthType#USERNAME_LOGIN} 路径使用；{@link AuthType#ACCESS_CODE} 类 token 直接走 yaml 绝对寿命。
     */
    long resolveTtlSeconds() {
        long now = System.currentTimeMillis();
        long cached = cachedTtlSeconds;
        if (cached > 0 && now - cachedTtlAt < TTL_CACHE_MS) {
            return cached;
        }
        long resolved = readTtlFromSettings();
        if (resolved <= 0) {
            resolved = accessTokenTtl != null ? accessTokenTtl : 0L;
        }
        cachedTtlSeconds = resolved;
        cachedTtlAt = now;
        return resolved;
    }

    private long readTtlFromSettings() {
        try {
            if (settingsService == null) return -1L;
            Object v = settingsService.getValueByCategoryAndKey(CategoryEnum.LOGIN, KeyEnum.TOKEN_IDLE_TIMEOUT_MINUTES);
            if (v == null || StringUtils.isBlank(v.toString())) return -1L;
            long minutes = Long.parseLong(v.toString().trim());
            return minutes > 0 ? minutes * 60L : -1L;
        } catch (Exception e) {
            log.warn("Failed to read token idle timeout from Settings, err={}", e.getMessage());
            return -1L;
        }
    }

    /**
     * 写入节流：仅当距上次活跃时间已超过 TTL 的 1/10 时才更新 last_updated，
     * 避免每次请求都写库；只要 token 在窗口内继续被使用，过期时间就会被持续延后。
     */
    private void refreshLastUpdatedIfNeeded(String accessToken, Date lastActiveDate, long ttlSeconds, long now) {
        long elapsed = now - lastActiveDate.getTime();
        long threshold = (ttlSeconds * 1000) / 10;
        if (threshold <= 0 || elapsed < threshold) {
            return;
        }
        try {
            accessTokenRepository.getMongoOperations().updateFirst(
                    Query.query(Criteria.where("_id").is(accessToken)),
                    new Update().set("last_updated", new Date(now)),
                    AccessTokenEntity.class
            );
        } catch (Exception e) {
            log.warn("Failed to refresh access token last_updated, err={}", e.getMessage());
        }
    }

    public AccessTokenDto save(User user) {
        AccessTokenDto accessTokenDto = null;
        if (user != null) {
            String id = UUIDUtil.get64UUID();
            Date now = new Date();
            AccessTokenEntity accessTokenEntity = new AccessTokenEntity(id, resolveTtlSeconds(), now, user.getId(), now, AuthType.USERNAME_LOGIN.getValue());
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
                log.error("Logout and add operation log exception", e);
            }
        }

        return result.getDeletedCount();
    }

    public long removeAccessTokenByAuthType(ObjectId userId, String authType) {
        DeleteResult result = accessTokenRepository.getMongoOperations().remove(Query.query(Criteria.where("authType").is(authType)
                .and("userId").is(userId)), AccessTokenEntity.class);
        if (result.getDeletedCount() > 0) {
            //增加操作日志
            try {
                UserDetail userDetail = userService.loadUserById(userId);
                userLogService.addUserLog(Modular.SYSTEM, Operation.LOGOUT, userDetail, "");
            } catch (Exception e) {
                log.error("Logout and add operation log exception", e);
            }
        }

        return result.getDeletedCount();
    }
}
