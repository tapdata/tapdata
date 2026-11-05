package com.tapdata.tm.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author knight
 * @Date 2025/9/17 11:53
 */

public class MonitorConfigListener implements ApplicationListener<ApplicationEnvironmentPreparedEvent> {

    private static final String EXPOSURE_INCLUDE = "management.endpoints.web.exposure.include";
    private static final String EXPOSURE_EXCLUDE = "management.endpoints.web.exposure.exclude";
    private static final String DISCOVERY_ENABLED = "management.endpoints.web.discovery.enabled";
    private static final String ACCESS_DEFAULT = "management.endpoints.access.default";
    private static final String ACCESS_MAX_PERMITTED = "management.endpoints.access.max-permitted";

    private final Logger log = LoggerFactory.getLogger(MonitorConfigListener.class);

    @Override
    public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
        ConfigurableEnvironment environment = event.getEnvironment();

        String monitorEnable = environment.getProperty("TAPDATA_MONITOR_ENABLE");
        log.info("start MonitorConfigListener monitor Enable : {}", monitorEnable);
        boolean isMonitorEnabled = "true".equalsIgnoreCase(monitorEnable);

        Map<String, Object> properties = new HashMap<>();
        if (isMonitorEnabled) {
            properties.put(EXPOSURE_INCLUDE, "prometheus, info, health");
        } else {
            // 监控未开启时不暴露任何 Actuator 端点：仅暴露 health 名义入口，同时用 exclude 通配、
            // 关闭发现页并收紧访问级别，避免未认证请求读取 /actuator 与 /actuator/info。
            properties.put(EXPOSURE_INCLUDE, "health");
            properties.put(EXPOSURE_EXCLUDE, "*");
            properties.put(DISCOVERY_ENABLED, false);
            properties.put(ACCESS_DEFAULT, "none");
            properties.put(ACCESS_MAX_PERMITTED, "none");
        }

        // 添加到环境配置中，优先级高于application.yml
        MutablePropertySources propertySources = environment.getPropertySources();
        propertySources.addFirst(new MapPropertySource("dynamicMonitorConfig", properties));

        log.info("end monitorConfigListener monitor enabled: {}, endpoints include: {}, endpoints exclude: {}, discovery enabled: {}",
                isMonitorEnabled, properties.get(EXPOSURE_INCLUDE), properties.get(EXPOSURE_EXCLUDE),
                properties.get(DISCOVERY_ENABLED));
    }
}