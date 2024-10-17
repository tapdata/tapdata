package com.tapdata.mongo;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.LoginResp;
import com.tapdata.entity.User;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class HttpClientMongoOperatorTest {
    @Nested
    class ValidateTokenTest{
        HttpClientMongoOperator httpClientMongoOperator;
        ConfigurationCenter configurationCenter;
        @BeforeEach
        void init(){
            httpClientMongoOperator = mock(HttpClientMongoOperator.class);
            configurationCenter = new ConfigurationCenter();
            ReflectionTestUtils.setField(httpClientMongoOperator, "configCenter", configurationCenter);
        }
        @DisplayName("test validateToken when ExpiredTimestamp less than refreshBufferTime")
        @Test
        void test1(){
            doCallRealMethod().when(httpClientMongoOperator).validateToken();
            long ttl = 300;
            LoginResp loginResp = new LoginResp();
            loginResp.setExpiredTimestamp(System.currentTimeMillis() + ttl * 1000);
            loginResp.setTtl(ttl);

            configurationCenter.putConfig(ConfigurationCenter.LOGIN_INFO,loginResp);
            httpClientMongoOperator.validateToken();
            verify(httpClientMongoOperator,times(0)).refreshToken();
        }

        @DisplayName("test validateToken when ExpiredTimestamp Greater than refreshBufferTime")
        @Test
        void test2(){
            doCallRealMethod().when(httpClientMongoOperator).validateToken();
            long ttl = 300;
            LoginResp loginResp = new LoginResp();
            loginResp.setTtl(ttl);
            loginResp.setExpiredTimestamp(0);
            configurationCenter.putConfig(ConfigurationCenter.LOGIN_INFO, loginResp);
            httpClientMongoOperator.validateToken();
            verify(httpClientMongoOperator,times(1)).refreshToken();
        }
        @DisplayName("test validateToken when login info is null")
        @Test
        void test3(){
            doCallRealMethod().when(httpClientMongoOperator).validateToken();
            httpClientMongoOperator.validateToken();
            verify(httpClientMongoOperator,times(1)).refreshToken();
        }
    }
    @Nested
    class RefreshTokenClass{
        HttpClientMongoOperator httpClientMongoOperator;
        ConfigurationCenter configurationCenter;
        String accessCode = "accessCode";
        RestTemplateOperator restTemplateOperator;
        Logger logger;

        @BeforeEach
        void init() {
            httpClientMongoOperator = mock(HttpClientMongoOperator.class);
            restTemplateOperator = mock(RestTemplateOperator.class);
            logger = mock(Logger.class);
            configurationCenter = new ConfigurationCenter();
            ReflectionTestUtils.setField(httpClientMongoOperator, "configCenter", configurationCenter);
            configurationCenter.putConfig(ConfigurationCenter.ACCESS_CODE, accessCode);
            ReflectionTestUtils.setField(httpClientMongoOperator, "restTemplateOperator", restTemplateOperator);
            ReflectionTestUtils.setField(httpClientMongoOperator, "logger", logger);
        }

        @DisplayName("test refresh success")
        @Test
        void test1() {
            String token = "testAccessToken";
            LoginResp loginResp = new LoginResp();
            loginResp.setCreated("2024-10-17T08:00:00Z");
            loginResp.setTtl(2000L);
            loginResp.setId(token);
            loginResp.setUserId("testUserId");
            when(restTemplateOperator.postOne(anyMap(), anyString(), any())).thenReturn(loginResp);
            User user = new User();
            user.setId("userId");
            when(httpClientMongoOperator.findOne(any(Query.class), anyString(), any())).thenReturn(user);
            doCallRealMethod().when(httpClientMongoOperator).refreshToken();
            httpClientMongoOperator.refreshToken();
            assertEquals(token, configurationCenter.getConfig(ConfigurationCenter.TOKEN));
        }
        @DisplayName("test refresh token error")
        @Test
        void test2() {
            when(restTemplateOperator.postOne(anyMap(), anyString(), any())).thenThrow(new RuntimeException("can not get token"));
            User user = new User();
            user.setId("userId");
            when(httpClientMongoOperator.findOne(any(Query.class), anyString(), any())).thenReturn(user);
            doCallRealMethod().when(httpClientMongoOperator).refreshToken();
            httpClientMongoOperator.refreshToken();
            verify(logger,times(6)).error(anyString(),anyString(),any(Exception.class));
        }
    }
}
