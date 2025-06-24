package com.tapdata.tm.config;

import com.tapdata.tm.oauth2.filter.OAuth2JsonSupportFilter;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

/**
 * OAuth2过滤器配置
 */
@Configuration
public class OAuth2FilterConfig {

    /**
     * 注册OAuth2 JSON支持过滤器
     */
    @Bean
    public FilterRegistrationBean<OAuth2JsonSupportFilter> oAuth2JsonSupportFilterRegistration() {
        FilterRegistrationBean<OAuth2JsonSupportFilter> registration = new FilterRegistrationBean<>();
        registration.setFilter(new OAuth2JsonSupportFilter());
        registration.addUrlPatterns("/oauth/token");
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
        registration.setName("OAuth2JsonSupportFilter");
        return registration;
    }
}
