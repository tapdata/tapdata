package com.tapdata.tm.actuator;

import com.tapdata.tm.listener.MonitorConfigListener;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.util.ClassUtils;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * HTTP 黑盒验证：真实启动 Servlet Web 容器与 Actuator 自动配置，暴露策略来自生产代码 MonitorConfigListener。
 *
 * 覆盖三组场景：
 * 1) 监控关闭（TAPDATA_MONITOR_ENABLE 未设置）时，发现页与任何端点对匿名请求均不可达；
 * 2) 同一进程改用修复前的策略（include=info）启动时发现页与 info 返回 200，证明用例具备区分能力；
 * 3) 监控开启时 prometheus/health/info 与发现页保持 200，既有采集能力不退化。
 */
class ActuatorExposureIT {

    private static final String ACTUATOR = "/actuator";
    private static final String ACTUATOR_INFO = "/actuator/info";
    private static final String ACTUATOR_HEALTH = "/actuator/health";
    private static final String ACTUATOR_ENV = "/actuator/env";
    private static final String ACTUATOR_PROMETHEUS = "/actuator/prometheus";

    /** 需要排除的自动配置：外部中间件与默认安全链，避免环境依赖和 401 干扰暴露断言。 */
    private static final String[] EXCLUDED_AUTO_CONFIGURATIONS = {
            "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
            "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
            "org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration",
            "org.springframework.boot.mongodb.autoconfigure.MongoAutoConfiguration",
            "org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration",
            "org.springframework.boot.data.mongodb.autoconfigure.DataMongoAutoConfiguration",
            "org.springframework.boot.autoconfigure.data.mongo.MongoRepositoriesAutoConfiguration",
            "org.springframework.boot.data.mongodb.autoconfigure.DataMongoRepositoriesAutoConfiguration",
            "org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration",
            "org.springframework.boot.security.autoconfigure.SecurityAutoConfiguration",
            "org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration",
            "org.springframework.boot.security.autoconfigure.UserDetailsServiceAutoConfiguration",
            // Actuator 自带的默认安全链会把端点统一要求认证并返回 401，掩盖真实暴露策略，一并排除。
            "org.springframework.boot.actuate.autoconfigure.security.servlet.ManagementWebSecurityAutoConfiguration",
            "org.springframework.boot.security.autoconfigure.actuate.web.servlet.ManagementWebSecurityAutoConfiguration",
            "org.springframework.boot.autoconfigure.security.servlet.SecurityFilterAutoConfiguration",
            "org.springframework.boot.security.autoconfigure.web.servlet.SecurityFilterAutoConfiguration",
            // Boot 4 把“默认要求全站认证”的 SecurityFilterChain 放在 ServletWebSecurityAutoConfiguration，
            // Boot 3 对应 SpringBootWebSecurityConfiguration，两者都必须排除，否则匿名请求会先被 401 拦截。
            "org.springframework.boot.security.autoconfigure.web.servlet.ServletWebSecurityAutoConfiguration",
            "org.springframework.boot.autoconfigure.security.servlet.SpringBootWebSecurityConfiguration"
    };

    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();

    /** 最小 Web 上下文：只保留 Web MVC 与 Actuator，断言只反映暴露策略本身。 */
    @Configuration
    @EnableAutoConfiguration
    static class ActuatorOnlyApplication {
    }

    /** 修复前的暴露策略，仅用于证明用例能区分缺陷。 */
    private static class LegacyExposureListener implements ApplicationListener<ApplicationEnvironmentPreparedEvent> {
        @Override
        public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
            ConfigurableEnvironment environment = event.getEnvironment();
            Map<String, Object> properties = new HashMap<>();
            properties.put("management.endpoints.web.exposure.include", "info");
            environment.getPropertySources().addFirst(new MapPropertySource("legacyMonitorConfig", properties));
        }
    }

    private List<String> presentExcludes() {
        List<String> excludes = new ArrayList<>();
        for (String name : EXCLUDED_AUTO_CONFIGURATIONS) {
            if (ClassUtils.isPresent(name, ActuatorExposureIT.class.getClassLoader())) {
                excludes.add(name);
            }
        }
        return excludes;
    }

    private ConfigurableApplicationContext start(ApplicationListener<ApplicationEnvironmentPreparedEvent> listener,
                                                 String monitorEnable) {
        SpringApplication application = new SpringApplication(ActuatorOnlyApplication.class);
        application.addListeners(listener);
        if (monitorEnable == null) {
            System.clearProperty("TAPDATA_MONITOR_ENABLE");
        } else {
            System.setProperty("TAPDATA_MONITOR_ENABLE", monitorEnable);
        }
        // 使用命令行参数而非默认属性：tm 模块自带的 yml 会以 server.port=3000 覆盖默认属性，
        // 这里必须用最高优先级参数把端口固定为随机端口，避免与运行中的实例冲突。
        List<String> args = new ArrayList<>();
        args.add("--server.port=0");
        args.add("--spring.main.banner-mode=off");
        args.add("--spring.jmx.enabled=false");
        args.add("--management.prometheus.metrics.export.enabled=true");
        // 最小上下文没有真实中间件，默认健康指示器会聚合为 DOWN(503)；关闭默认指示器，
        // 使 health 只反映“端点是否可达”，与本用例断言目标一致。
        args.add("--management.health.defaults.enabled=false");
        if (!presentExcludes().isEmpty()) {
            args.add("--spring.autoconfigure.exclude=" + String.join(",", presentExcludes()));
        }
        return application.run(args.toArray(new String[0]));
    }

    private int port(ConfigurableApplicationContext context) {
        return ((WebServerApplicationContext) context).getWebServer().getPort();
    }

    private int status(int port, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode();
    }

    @Test
    @DisplayName("监控关闭：/actuator 发现页与 info/health/env 端点对匿名请求均不可达")
    void shouldNotExposeAnyEndpointWhenMonitorDisabled() throws Exception {
        try (ConfigurableApplicationContext context = start(new MonitorConfigListener(), null)) {
            int port = port(context);
            assertEquals(404, status(port, ACTUATOR), "发现页不得匿名可达");
            assertEquals(404, status(port, ACTUATOR_INFO), "info 端点不得匿名可达");
            assertEquals(404, status(port, ACTUATOR_HEALTH), "health 端点不得匿名可达");
            assertEquals(404, status(port, ACTUATOR_ENV), "env 端点不得匿名可达");
        }
    }

    @Test
    @DisplayName("修复前暴露策略（include=info）下发现页与 info 返回 200，证明用例可区分缺陷")
    void shouldReproduceExposureWithLegacyPolicy() throws Exception {
        try (ConfigurableApplicationContext context = start(new LegacyExposureListener(), null)) {
            int port = port(context);
            assertEquals(200, status(port, ACTUATOR), "修复前发现页应为 200，与本用例预期一致");
            assertEquals(200, status(port, ACTUATOR_INFO), "修复前 info 应为 200，与本用例预期一致");
        }
    }

    @Test
    @DisplayName("监控开启：发现页与 prometheus、health、info 保持 200，既有采集能力不退化")
    void shouldKeepMonitorEndpointsWhenEnabled() throws Exception {
        try (ConfigurableApplicationContext context = start(new MonitorConfigListener(), "true")) {
            int port = port(context);
            assertEquals(200, status(port, ACTUATOR), "监控开启时发现页应可达");
            assertEquals(200, status(port, ACTUATOR_PROMETHEUS), "prometheus 端点应可达");
            assertEquals(200, status(port, ACTUATOR_HEALTH), "health 端点应可达");
            assertEquals(200, status(port, ACTUATOR_INFO), "info 端点应可达");
        } finally {
            System.clearProperty("TAPDATA_MONITOR_ENABLE");
        }
    }
}
