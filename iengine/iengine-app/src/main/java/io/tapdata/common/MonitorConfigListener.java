package io.tapdata.common;

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

    private Logger log = LoggerFactory.getLogger(MonitorConfigListener.class);

    @Override
    public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
        ConfigurableEnvironment environment = event.getEnvironment();

        String monitorEnable = environment.getProperty("TAPDATA_MONITOR_ENABLE");
        log.info("start MonitorConfigListener monitor Enable : {}", monitorEnable);
        boolean isMonitorEnabled = "true".equalsIgnoreCase(monitorEnable);

        Map<String, Object> properties = new HashMap<>();
        if (isMonitorEnabled) {
            properties.put("management.endpoints.web.exposure.include", "prometheus, info, health");
        } else {
            properties.put("management.endpoints.web.exposure.include", "info");
        }

        // 添加到环境配置中，优先级高于application.yml
        MutablePropertySources propertySources = environment.getPropertySources();
        propertySources.addFirst(new MapPropertySource("dynamicMonitorConfig", properties));

        log.info("end monitorConfigListener monitor enabled: " + isMonitorEnabled +
                ", endpoints include: " + properties.get("management.endpoints.web.exposure.include"));
    }
}