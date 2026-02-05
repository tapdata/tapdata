package com.tapdata.tm.config;

import com.tapdata.tm.oauth2.filter.OAuth2JsonSupportFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.core.Ordered;

import static org.junit.jupiter.api.Assertions.*;

class OAuth2FilterConfigTest {

    private OAuth2FilterConfig config;

    @BeforeEach
    void setUp() {
        config = new OAuth2FilterConfig();
    }

    @Test
    void testOAuth2JsonSupportFilterRegistrationNotNull() {
        FilterRegistrationBean<OAuth2JsonSupportFilter> registration = 
            config.oAuth2JsonSupportFilterRegistration();
        
        assertNotNull(registration);
    }

    @Test
    void testFilterIsConfigured() {
        FilterRegistrationBean<OAuth2JsonSupportFilter> registration = 
            config.oAuth2JsonSupportFilterRegistration();
        
        assertNotNull(registration.getFilter());
        assertTrue(registration.getFilter() instanceof OAuth2JsonSupportFilter);
    }

    @Test
    void testUrlPatternsAreConfigured() {
        FilterRegistrationBean<OAuth2JsonSupportFilter> registration = 
            config.oAuth2JsonSupportFilterRegistration();
        
        assertNotNull(registration.getUrlPatterns());
        assertTrue(registration.getUrlPatterns().contains("/oauth/token"));
        assertEquals(1, registration.getUrlPatterns().size());
    }

    @Test
    void testFilterOrderIsHighestPrecedence() {
        FilterRegistrationBean<OAuth2JsonSupportFilter> registration = 
            config.oAuth2JsonSupportFilterRegistration();
        
        assertEquals(Ordered.HIGHEST_PRECEDENCE, registration.getOrder());
    }

    @Test
    void testFilterNameIsConfigured() {
        FilterRegistrationBean<OAuth2JsonSupportFilter> registration = 
            config.oAuth2JsonSupportFilterRegistration();
        
        assertEquals("OAuth2JsonSupportFilter", registration.getFilterName());
    }

    @Test
    void testMultipleCallsCreateDifferentInstances() {
        FilterRegistrationBean<OAuth2JsonSupportFilter> registration1 = 
            config.oAuth2JsonSupportFilterRegistration();
        FilterRegistrationBean<OAuth2JsonSupportFilter> registration2 = 
            config.oAuth2JsonSupportFilterRegistration();
        
        assertNotSame(registration1, registration2);
        assertNotSame(registration1.getFilter(), registration2.getFilter());
    }

    @Test
    void testAllPropertiesAreSetCorrectly() {
        FilterRegistrationBean<OAuth2JsonSupportFilter> registration = 
            config.oAuth2JsonSupportFilterRegistration();
        
        // 验证所有属性都正确设置
        assertAll("Filter Registration Properties",
            () -> assertNotNull(registration.getFilter(), "Filter should not be null"),
            () -> assertEquals("OAuth2JsonSupportFilter", registration.getFilterName(), "Name should be OAuth2JsonSupportFilter"),
            () -> assertTrue(registration.getUrlPatterns().contains("/oauth/token"), "URL pattern should contain /oauth/token"),
            () -> assertEquals(Ordered.HIGHEST_PRECEDENCE, registration.getOrder(), "Order should be HIGHEST_PRECEDENCE")
        );
    }
}