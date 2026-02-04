package com.tapdata.tm.config;

import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AuthorizationConfigTest {

    @Nested
    class clusterIdTest {
        @Test
        void testNormal() {
            SettingsService settingsService = mock(SettingsService.class);
            AuthorizationConfig config = mock(AuthorizationConfig.class);
            JwtClaimsSet.Builder builder = mock(JwtClaimsSet.Builder.class);
            when(builder.claim(anyString(), anyString())).thenReturn(builder);
            doCallRealMethod().when(config).useClusterId(any(JwtClaimsSet.Builder.class));
            when(settingsService.getByKey("cluster")).thenReturn(null);
            ReflectionTestUtils.setField(config, "settingsService", settingsService);
            config.useClusterId(builder);
        }
        @Test
        void testNormal0() {
            SettingsService settingsService = mock(SettingsService.class);
            AuthorizationConfig config = mock(AuthorizationConfig.class);
            JwtClaimsSet.Builder builder = mock(JwtClaimsSet.Builder.class);
            when(builder.claim(anyString(), anyString())).thenReturn(builder);
            doCallRealMethod().when(config).useClusterId(null);
            when(settingsService.getByKey("cluster")).thenReturn(null);
            config.useClusterId(null);
        }
        @Test
        void testNormal1() {
            Settings cluster = mock(Settings.class);
            when(cluster.getId()).thenReturn("cluster");
            SettingsService settingsService = mock(SettingsService.class);
            AuthorizationConfig config = mock(AuthorizationConfig.class);
            doCallRealMethod().when(config).useClusterId(any(JwtClaimsSet.Builder.class));
            JwtClaimsSet.Builder builder = mock(JwtClaimsSet.Builder.class);
            when(builder.claim(anyString(), anyString())).thenReturn(builder);
            doCallRealMethod().when(config).useClusterId(any(JwtClaimsSet.Builder.class));
            when(settingsService.getByKey("cluster")).thenReturn(cluster);
            ReflectionTestUtils.setField(config, "settingsService", settingsService);
            config.useClusterId(builder);
        }
        @Test
        void testNormal2() {
            Settings cluster = mock(Settings.class);
            when(cluster.getId()).thenReturn(null);
            SettingsService settingsService = mock(SettingsService.class);
            AuthorizationConfig config = mock(AuthorizationConfig.class);
            doCallRealMethod().when(config).useClusterId(any(JwtClaimsSet.Builder.class));
            JwtClaimsSet.Builder builder = mock(JwtClaimsSet.Builder.class);
            when(builder.claim(anyString(), anyString())).thenReturn(builder);
            doCallRealMethod().when(config).useClusterId(any(JwtClaimsSet.Builder.class));
            when(settingsService.getByKey("cluster")).thenReturn(cluster);
            ReflectionTestUtils.setField(config, "settingsService", settingsService);
            config.useClusterId(builder);
        }
    }
}