package com.tapdata.tm.accessToken.service;

import com.mongodb.client.result.DeleteResult;
import com.tapdata.tm.accessToken.dto.AccessTokenDto;
import com.tapdata.tm.accessToken.dto.AuthType;
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
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class AccessTokenServiceImplTest {
    private AccessTokenServiceImpl accessTokenServiceImpl;
    private AccessTokenRepository accessTokenRepository;
    private UserServiceImpl userService;
    private UserLogService userLogService;

    @BeforeEach
    void beforeEach() {
        accessTokenServiceImpl = new AccessTokenServiceImpl();
        accessTokenRepository = mock(AccessTokenRepository.class);
        userService = mock(UserServiceImpl.class);
        userLogService = mock(UserLogService.class);
        ReflectionTestUtils.setField(accessTokenServiceImpl, "accessTokenRepository", accessTokenRepository);
        ReflectionTestUtils.setField(accessTokenServiceImpl, "userService", userService);
        ReflectionTestUtils.setField(accessTokenServiceImpl, "userLogService", userLogService);
    }
    @Test
    void saveTest() {
        User user = mock(User.class);
        when(accessTokenRepository.getMongoOperations()).thenReturn(mock(MongoTemplate.class));
        AccessTokenDto actual = accessTokenServiceImpl.save(user);
        assertEquals(AuthType.USERNAME_LOGIN.getValue(), actual.getAuthType());
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
