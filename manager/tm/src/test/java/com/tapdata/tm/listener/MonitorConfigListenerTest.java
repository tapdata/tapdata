package com.tapdata.tm.listener;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 监控开关对应的 Actuator 暴露策略：监控关闭时不得暴露任何 Web 端点，
 * 监控开启时保持 prometheus/info/health 的既有暴露。
 */
class MonitorConfigListenerTest {

    private static final String SOURCE_NAME = "dynamicMonitorConfig";

    private ConfigurableEnvironment environmentWith(String monitorEnable) {
        ConfigurableEnvironment environment = new StandardEnvironment();
        if (monitorEnable != null) {
            Map<String, Object> values = new HashMap<>();
            values.put("TAPDATA_MONITOR_ENABLE", monitorEnable);
            environment.getPropertySources().addFirst(new MapPropertySource("testInput", values));
        }
        return environment;
    }

    private PropertySource<?> apply(String monitorEnable) {
        ConfigurableEnvironment environment = environmentWith(monitorEnable);
        ApplicationEnvironmentPreparedEvent event =
                new ApplicationEnvironmentPreparedEvent(null, new SpringApplication(Object.class), new String[0], environment);
        new MonitorConfigListener().onApplicationEvent(event);
        PropertySource<?> source = environment.getPropertySources().get(SOURCE_NAME);
        assertNotNull(source, "监听器必须写入 dynamicMonitorConfig");
        return source;
    }

    @Test
    @DisplayName("监控开启时保持 prometheus、info、health 暴露，且不引入额外的关闭项")
    void shouldKeepMonitorEndpointsWhenEnabled() {
        PropertySource<?> source = apply("true");

        assertEquals("prometheus, info, health", source.getProperty("management.endpoints.web.exposure.include"));
        assertNull(source.getProperty("management.endpoints.web.exposure.exclude"));
        assertNull(source.getProperty("management.endpoints.web.discovery.enabled"));
        assertNull(source.getProperty("management.endpoints.access.default"));
        assertNull(source.getProperty("management.endpoints.access.max-permitted"));
    }

    @Test
    @DisplayName("监控关闭时关闭发现页与全部端点，不保留 info 暴露")
    void shouldCloseAllEndpointsWhenDisabled() {
        PropertySource<?> source = apply("false");

        assertEquals("health", source.getProperty("management.endpoints.web.exposure.include"));
        assertEquals("*", source.getProperty("management.endpoints.web.exposure.exclude"));
        assertEquals(Boolean.FALSE, source.getProperty("management.endpoints.web.discovery.enabled"));
        assertEquals("none", source.getProperty("management.endpoints.access.default"));
        assertEquals("none", source.getProperty("management.endpoints.access.max-permitted"));
    }

    @Test
    @DisplayName("未设置监控开关（企业版默认）时与显式关闭一致，不暴露 Actuator")
    void shouldCloseAllEndpointsWhenUnset() {
        PropertySource<?> source = apply(null);

        assertEquals("health", source.getProperty("management.endpoints.web.exposure.include"));
        assertEquals("*", source.getProperty("management.endpoints.web.exposure.exclude"));
        assertEquals(Boolean.FALSE, source.getProperty("management.endpoints.web.discovery.enabled"));
        assertEquals("none", source.getProperty("management.endpoints.access.default"));
        assertFalse("info".equals(source.getProperty("management.endpoints.web.exposure.include")),
                "关闭监控时不得再把 info 作为暴露入口");
    }
}
