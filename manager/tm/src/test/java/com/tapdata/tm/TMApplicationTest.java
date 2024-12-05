package com.tapdata.tm;

import com.tapdata.tm.user.service.UserService;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TMApplicationTest {
    @Test
    void buildPropertyTest() {
        UserService userService = mock(UserService.class);
        when(userService.getMongodbUri()).thenReturn("test_uri");
        when(userService.getServerPort()).thenReturn("test_port");
        when(userService.isSsl()).thenReturn("test_ssl");
        when(userService.getCaPath()).thenReturn("test_caPath");
        when(userService.getKeyPath()).thenReturn("test_keyPath");
        TMApplication.buildProperty(userService);
        assertEquals("test_uri", CommonUtils.getProperty("tapdata_proxy_mongodb_uri"));
        assertEquals("test_port", CommonUtils.getProperty("tapdata_proxy_server_port"));
        assertEquals("test_ssl", CommonUtils.getProperty("tapdata_proxy_mongodb_ssl"));
        assertEquals("test_caPath", CommonUtils.getProperty("tapdata_proxy_mongodb_caPath"));
        assertEquals("test_keyPath", CommonUtils.getProperty("tapdata_proxy_mongodb_keyPath"));
        System.clearProperty("tapdata_proxy_mongodb_uri");
        System.clearProperty("tapdata_proxy_server_port");
        System.clearProperty("tapdata_proxy_mongodb_ssl");
        System.clearProperty("tapdata_proxy_mongodb_caPath");
        System.clearProperty("tapdata_proxy_mongodb_keyPath");
        assertNull(CommonUtils.getProperty("tapdata_proxy_mongodb_uri"));

    }
}
