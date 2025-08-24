package com.tapdata.tm.config.micrometer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.Arrays;
import java.util.Collections;
/**
 * @Author knight
 * @Date 2025/8/20 18:17
 */

@Configuration
public class ActuatorConfig {

    @Value("${tapdata.monitor.enable}")
    private Boolean monitorEnabled;

    @Bean
    @Primary
    public WebEndpointProperties webEndpointProperties() {

        if (monitorEnabled == null) {
            monitorEnabled = Boolean.valueOf(System.getenv("TAPDATA_MONITOR_ENABLE"));
        }
        WebEndpointProperties properties = new WebEndpointProperties();
        if (monitorEnabled) {
            properties.getExposure().getInclude().addAll(Arrays.asList("info", "prometheus"));
        } else {
            properties.getExposure().getInclude().addAll(Collections.singletonList("info"));
        }
        return properties;
    }
}
