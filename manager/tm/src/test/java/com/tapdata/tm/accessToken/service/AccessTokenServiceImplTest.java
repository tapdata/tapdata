package com.tapdata.tm.accessToken.service;

import com.mongodb.client.result.DeleteResult;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.accessToken.dto.AccessTokenDto;
import com.tapdata.tm.accessToken.dto.AuthType;
import com.tapdata.tm.accessToken.entity.AccessTokenEntity;
import com.tapdata.tm.accessToken.repository.AccessTokenRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.user.service.UserServiceImpl;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class AccessTokenServiceImplTest {
    private AccessTokenServiceImpl accessTokenServiceImpl;
    private AccessTokenRepository accessTokenRepository;
    private UserServiceImpl userService;
    private UserLogService userLogService;
    private SettingsService settingsService;

    @BeforeEach
    void beforeEach() {
        accessTokenServiceImpl = new AccessTokenServiceImpl();
        accessTokenRepository = mock(AccessTokenRepository.class);
        userService = mock(UserServiceImpl.class);
        userLogService = mock(UserLogService.class);
        settingsService = mock(SettingsService.class);
        ReflectionTestUtils.setField(accessTokenServiceImpl, "accessTokenRepository", accessTokenRepository);
        ReflectionTestUtils.setField(accessTokenServiceImpl, "userService", userService);
        ReflectionTestUtils.setField(accessTokenServiceImpl, "userLogService", userLogService);
        ReflectionTestUtils.setField(accessTokenServiceImpl, "settingsService", settingsService);
    }
    @Test
    void saveTest() {
        User user = mock(User.class);
        when(accessTokenRepository.getMongoOperations()).thenReturn(mock(MongoTemplate.class));
        AccessTokenDto actual = accessTokenServiceImpl.save(user);
        assertEquals(AuthType.USERNAME_LOGIN.getValue(), actual.getAuthType());
    }
    @Nested
    class validateTest {
        private static final long TTL_SECONDS = 1209600L;
        private MongoTemplate template;

        @BeforeEach
        void setUp() {
            ReflectionTestUtils.setField(accessTokenServiceImpl, "accessTokenTtl", TTL_SECONDS);
            template = mock(MongoTemplate.class);
            when(accessTokenRepository.getMongoOperations()).thenReturn(template);
        }

        @Test
        void testTokenNotFound() {
            when(template.findById("no-such", AccessTokenEntity.class)).thenReturn(null);
            assertNull(accessTokenServiceImpl.validate("no-such"));
            verify(template, never()).updateFirst(any(Query.class), any(Update.class), any(Class.class));
        }

        @Test
        void testInactiveExceedsTtlReturnsNull() {
            ObjectId userId = new ObjectId();
            Date longAgo = new Date(System.currentTimeMillis() - (TTL_SECONDS + 60) * 1000);
            AccessTokenEntity entity = new AccessTokenEntity("t1", TTL_SECONDS, longAgo, userId, longAgo, AuthType.USERNAME_LOGIN.getValue());
            when(template.findById("t1", AccessTokenEntity.class)).thenReturn(entity);
            assertNull(accessTokenServiceImpl.validate("t1"));
            verify(template, never()).updateFirst(any(Query.class), any(Update.class), any(Class.class));
        }

        @Test
        void testWithinWindowReturnsUserIdWithoutWrite() {
            ObjectId userId = new ObjectId();
            Date recent = new Date(System.currentTimeMillis() - 1000);
            AccessTokenEntity entity = new AccessTokenEntity("t2", TTL_SECONDS, recent, userId, recent, AuthType.USERNAME_LOGIN.getValue());
            when(template.findById("t2", AccessTokenEntity.class)).thenReturn(entity);
            assertEquals(userId, accessTokenServiceImpl.validate("t2"));
            verify(template, never()).updateFirst(any(Query.class), any(Update.class), any(Class.class));
        }

        @Test
        void testRefreshLastUpdatedWhenBeyondThreshold() {
            ObjectId userId = new ObjectId();
            Date created = new Date(System.currentTimeMillis() - TTL_SECONDS * 1000 / 2);
            AccessTokenEntity entity = new AccessTokenEntity("t3", TTL_SECONDS, created, userId, created, AuthType.USERNAME_LOGIN.getValue());
            when(template.findById("t3", AccessTokenEntity.class)).thenReturn(entity);
            assertEquals(userId, accessTokenServiceImpl.validate("t3"));
            verify(template, new Times(1)).updateFirst(any(Query.class), any(Update.class), eq(AccessTokenEntity.class));
        }

        @Test
        void testFallbackToCreatedWhenLastUpdatedNull() {
            ObjectId userId = new ObjectId();
            Date created = new Date(System.currentTimeMillis() - 1000);
            AccessTokenEntity entity = new AccessTokenEntity("t4", TTL_SECONDS, created, userId, null, AuthType.USERNAME_LOGIN.getValue());
            when(template.findById("t4", AccessTokenEntity.class)).thenReturn(entity);
            assertEquals(userId, accessTokenServiceImpl.validate("t4"));
        }

        @Test
        void testRefreshFailureDoesNotBreakValidation() {
            ObjectId userId = new ObjectId();
            Date created = new Date(System.currentTimeMillis() - TTL_SECONDS * 1000 / 2);
            AccessTokenEntity entity = new AccessTokenEntity("t5", TTL_SECONDS, created, userId, created, AuthType.USERNAME_LOGIN.getValue());
            when(template.findById("t5", AccessTokenEntity.class)).thenReturn(entity);
            when(template.updateFirst(any(Query.class), any(Update.class), any(Class.class))).thenThrow(new RuntimeException("db down"));
            assertEquals(userId, accessTokenServiceImpl.validate("t5"));
        }

        @Test
        void testAccessCodeIgnoresStaleLastUpdated() {
            // 引擎 token: lastUpdated 远在 Settings 不活跃超时之前，但 created 在 yaml TTL 内，应判定为有效
            ObjectId userId = new ObjectId();
            when(settingsService.getValueByCategoryAndKey(CategoryEnum.LOGIN, KeyEnum.TOKEN_IDLE_TIMEOUT_MINUTES)).thenReturn(60L);
            Date created = new Date(System.currentTimeMillis() - 1000);
            Date staleLastUpdated = new Date(System.currentTimeMillis() - 7L * 24 * 3600 * 1000);
            AccessTokenEntity entity = new AccessTokenEntity("m1", TTL_SECONDS, created, userId, staleLastUpdated, AuthType.ACCESS_CODE.getValue());
            when(template.findById("m1", AccessTokenEntity.class)).thenReturn(entity);
            assertEquals(userId, accessTokenServiceImpl.validate("m1"));
        }

        @Test
        void testAccessCodeExpiresByAbsoluteLifetime() {
            // 引擎 token: created 早于 yaml TTL，绝对寿命到期，应判定为无效
            ObjectId userId = new ObjectId();
            Date longAgo = new Date(System.currentTimeMillis() - (TTL_SECONDS + 60) * 1000);
            AccessTokenEntity entity = new AccessTokenEntity("m2", TTL_SECONDS, longAgo, userId, new Date(), AuthType.ACCESS_CODE.getValue());
            when(template.findById("m2", AccessTokenEntity.class)).thenReturn(entity);
            assertNull(accessTokenServiceImpl.validate("m2"));
        }

        @Test
        void testAccessCodeRefreshesLastUpdatedBeyondThreshold() {
            // 引擎 token: lastUpdated 超过 yaml TTL/10 阈值，应触发节流刷新（顺延 Mongo TTL 索引）
            ObjectId userId = new ObjectId();
            Date created = new Date(System.currentTimeMillis() - 1000);
            Date staleLastUpdated = new Date(System.currentTimeMillis() - TTL_SECONDS * 1000 / 2);
            AccessTokenEntity entity = new AccessTokenEntity("m3", TTL_SECONDS, created, userId, staleLastUpdated, AuthType.ACCESS_CODE.getValue());
            when(template.findById("m3", AccessTokenEntity.class)).thenReturn(entity);
            assertEquals(userId, accessTokenServiceImpl.validate("m3"));
            verify(template, new Times(1)).updateFirst(any(Query.class), any(Update.class), eq(AccessTokenEntity.class));
        }

        @Test
        void testNullAuthTypeFallsBackToSlidingWindow() {
            // 兼容历史数据：authType 缺失时按浏览器登录态处理（应用不活跃超时），属于安全保守的默认
            ObjectId userId = new ObjectId();
            Date recent = new Date(System.currentTimeMillis() - 1000);
            AccessTokenEntity entity = new AccessTokenEntity("n1", TTL_SECONDS, recent, userId, recent, null);
            when(template.findById("n1", AccessTokenEntity.class)).thenReturn(entity);
            assertEquals(userId, accessTokenServiceImpl.validate("n1"));
        }

        @Test
        void testPassiveRequestSkipsRefresh() {
            // 被动请求（X-User-Activity: 0）：即使已过 TTL/10 阈值，也不应触发 last_updated 节流刷新
            ObjectId userId = new ObjectId();
            Date created = new Date(System.currentTimeMillis() - TTL_SECONDS * 1000 / 2);
            AccessTokenEntity entity = new AccessTokenEntity("p1", TTL_SECONDS, created, userId, created, AuthType.USERNAME_LOGIN.getValue());
            when(template.findById("p1", AccessTokenEntity.class)).thenReturn(entity);
            assertEquals(userId, accessTokenServiceImpl.validate("p1", false));
            verify(template, never()).updateFirst(any(Query.class), any(Update.class), any(Class.class));
        }

        @Test
        void testPassiveRequestStillExpires() {
            // 被动请求不应反过来"豁免"过期判定：超过滑动窗口仍要返回 null
            ObjectId userId = new ObjectId();
            Date longAgo = new Date(System.currentTimeMillis() - (TTL_SECONDS + 60) * 1000);
            AccessTokenEntity entity = new AccessTokenEntity("p2", TTL_SECONDS, longAgo, userId, longAgo, AuthType.USERNAME_LOGIN.getValue());
            when(template.findById("p2", AccessTokenEntity.class)).thenReturn(entity);
            assertNull(accessTokenServiceImpl.validate("p2", false));
            verify(template, never()).updateFirst(any(Query.class), any(Update.class), any(Class.class));
        }

        @Test
        void testPassiveAccessCodeSkipsRefresh() {
            // 引擎 token 的被动场景（理论上不会发生，但保持语义一致）：也跳过 last_updated 顺延
            ObjectId userId = new ObjectId();
            Date created = new Date(System.currentTimeMillis() - 1000);
            Date staleLastUpdated = new Date(System.currentTimeMillis() - TTL_SECONDS * 1000 / 2);
            AccessTokenEntity entity = new AccessTokenEntity("p3", TTL_SECONDS, created, userId, staleLastUpdated, AuthType.ACCESS_CODE.getValue());
            when(template.findById("p3", AccessTokenEntity.class)).thenReturn(entity);
            assertEquals(userId, accessTokenServiceImpl.validate("p3", false));
            verify(template, never()).updateFirst(any(Query.class), any(Update.class), any(Class.class));
        }

        @Test
        void testSingleArgValidateDefaultsToActive() {
            // 向后兼容：单参 validate 等价于 countAsActivity=true
            ObjectId userId = new ObjectId();
            Date created = new Date(System.currentTimeMillis() - TTL_SECONDS * 1000 / 2);
            AccessTokenEntity entity = new AccessTokenEntity("p4", TTL_SECONDS, created, userId, created, AuthType.USERNAME_LOGIN.getValue());
            when(template.findById("p4", AccessTokenEntity.class)).thenReturn(entity);
            assertEquals(userId, accessTokenServiceImpl.validate("p4"));
            verify(template, new Times(1)).updateFirst(any(Query.class), any(Update.class), eq(AccessTokenEntity.class));
        }
    }

    @Nested
    class resolveTtlSecondsTest {
        @Test
        void testSettingsTakesPrecedenceWhenNumber() {
            ReflectionTestUtils.setField(accessTokenServiceImpl, "accessTokenTtl", 1209600L);
            when(settingsService.getValueByCategoryAndKey(CategoryEnum.LOGIN, KeyEnum.TOKEN_IDLE_TIMEOUT_MINUTES)).thenReturn(60L);
            assertEquals(3600L, accessTokenServiceImpl.resolveTtlSeconds());
        }

        @Test
        void testSettingsTakesPrecedenceWhenString() {
            ReflectionTestUtils.setField(accessTokenServiceImpl, "accessTokenTtl", 1209600L);
            when(settingsService.getValueByCategoryAndKey(CategoryEnum.LOGIN, KeyEnum.TOKEN_IDLE_TIMEOUT_MINUTES)).thenReturn("3600");
            assertEquals(216000L, accessTokenServiceImpl.resolveTtlSeconds());
        }

        @Test
        void testFallbackToYamlWhenSettingsMissing() {
            ReflectionTestUtils.setField(accessTokenServiceImpl, "accessTokenTtl", 1209600L);
            when(settingsService.getValueByCategoryAndKey(CategoryEnum.LOGIN, KeyEnum.TOKEN_IDLE_TIMEOUT_MINUTES)).thenReturn(null);
            assertEquals(1209600L, accessTokenServiceImpl.resolveTtlSeconds());
        }

        @Test
        void testFallbackToYamlWhenSettingsInvalid() {
            ReflectionTestUtils.setField(accessTokenServiceImpl, "accessTokenTtl", 1209600L);
            when(settingsService.getValueByCategoryAndKey(CategoryEnum.LOGIN, KeyEnum.TOKEN_IDLE_TIMEOUT_MINUTES)).thenReturn("not-a-number");
            assertEquals(1209600L, accessTokenServiceImpl.resolveTtlSeconds());
        }

        @Test
        void testFallbackWhenSettingsThrows() {
            ReflectionTestUtils.setField(accessTokenServiceImpl, "accessTokenTtl", 1209600L);
            when(settingsService.getValueByCategoryAndKey(CategoryEnum.LOGIN, KeyEnum.TOKEN_IDLE_TIMEOUT_MINUTES))
                    .thenThrow(new RuntimeException("db down"));
            assertEquals(1209600L, accessTokenServiceImpl.resolveTtlSeconds());
        }

        @Test
        void testResultIsCachedForShortPeriod() {
            ReflectionTestUtils.setField(accessTokenServiceImpl, "accessTokenTtl", 1209600L);
            when(settingsService.getValueByCategoryAndKey(CategoryEnum.LOGIN, KeyEnum.TOKEN_IDLE_TIMEOUT_MINUTES)).thenReturn(60L);
            accessTokenServiceImpl.resolveTtlSeconds();
            accessTokenServiceImpl.resolveTtlSeconds();
            accessTokenServiceImpl.resolveTtlSeconds();
            verify(settingsService, new Times(1)).getValueByCategoryAndKey(CategoryEnum.LOGIN, KeyEnum.TOKEN_IDLE_TIMEOUT_MINUTES);
        }
    }

    @Nested
    class removeAccessTokenTest {
        @Test
        void testRemoveAccessTokenNormal() {
            String accessToken = "accessToken";
            UserDetail userDetail = mock(UserDetail.class);
            MongoTemplate template = mock(MongoTemplate.class);
            when(accessTokenRepository.getMongoOperations()).thenReturn(template);
            DeleteResult result = mock(DeleteResult.class);
            when(template.remove(any(Query.class), any(Class.class))).thenReturn(result);
            when(result.getDeletedCount()).thenReturn(1L);
            accessTokenServiceImpl.removeAccessToken(accessToken, userDetail);
            verify(userLogService, new Times(1)).addUserLog(any(Modular.class), any(Operation.class), any(UserDetail.class), anyString());
        }
        @Test
        void testRemoveAccessTokenWithEx() {
            String accessToken = "accessToken";
            UserDetail userDetail = mock(UserDetail.class);
            MongoTemplate template = mock(MongoTemplate.class);
            when(accessTokenRepository.getMongoOperations()).thenReturn(template);
            DeleteResult result = mock(DeleteResult.class);
            when(template.remove(any(Query.class), any(Class.class))).thenReturn(result);
            when(result.getDeletedCount()).thenReturn(1L);
            doThrow(RuntimeException.class).when(userLogService).addUserLog(any(Modular.class), any(Operation.class), any(UserDetail.class), anyString());
            accessTokenServiceImpl.removeAccessToken(accessToken, userDetail);
            verify(userLogService, new Times(1)).addUserLog(any(Modular.class), any(Operation.class), any(UserDetail.class), anyString());
        }
    }

    @Nested
    class removeAccessTokenByAuthTypeTest {
        @Test
        void testRemoveAccessTokenByAuthTypeNormal() {
            ObjectId userId = mock(ObjectId.class);
            String authType = AuthType.USERNAME_LOGIN.getValue();
            MongoTemplate template = mock(MongoTemplate.class);
            when(accessTokenRepository.getMongoOperations()).thenReturn(template);
            DeleteResult result = mock(DeleteResult.class);
            when(template.remove(any(Query.class), any(Class.class))).thenReturn(result);
            when(result.getDeletedCount()).thenReturn(1L);
            accessTokenServiceImpl.removeAccessTokenByAuthType(userId, authType);
            verify(userService, new Times(1)).loadUserById(userId);
            verify(userLogService, new Times(1)).addUserLog(any(Modular.class), any(Operation.class), eq(null), anyString());
        }
        @Test
        void testRemoveAccessTokenByAuthTypeWithEx() {
            ObjectId userId = mock(ObjectId.class);
            String authType = AuthType.USERNAME_LOGIN.getValue();
            MongoTemplate template = mock(MongoTemplate.class);
            when(accessTokenRepository.getMongoOperations()).thenReturn(template);
            DeleteResult result = mock(DeleteResult.class);
            when(template.remove(any(Query.class), any(Class.class))).thenReturn(result);
            when(result.getDeletedCount()).thenReturn(1L);
            doThrow(RuntimeException.class).when(userLogService).addUserLog(any(Modular.class), any(Operation.class), eq(null), anyString());
            accessTokenServiceImpl.removeAccessTokenByAuthType(userId, authType);
            verify(userService, new Times(1)).loadUserById(userId);
            verify(userLogService, new Times(1)).addUserLog(any(Modular.class), any(Operation.class), eq(null), anyString());
        }
    }
}
