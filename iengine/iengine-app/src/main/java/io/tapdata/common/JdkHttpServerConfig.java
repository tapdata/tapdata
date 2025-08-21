package io.tapdata.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.SystemHealth;
import org.springframework.boot.actuate.metrics.export.prometheus.PrometheusOutputFormat;
import org.springframework.boot.actuate.metrics.export.prometheus.PrometheusScrapeEndpoint;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/**
 * 使用JDK内置HttpServer暴露actuator端点的配置类
 * 通过management.jdk-server.enabled配置开关控制
 */
@Configuration
public class JdkHttpServerConfig implements ApplicationListener<ApplicationReadyEvent> {

    private final HealthEndpoint healthEndpoint;
    private final PrometheusScrapeEndpoint prometheusScrapeEndpoint;
    private final ObjectMapper objectMapper;
    //是否开启monitor监控
    private boolean enabled = Boolean.parseBoolean(System.getenv().getOrDefault("TAPDATA_MONITOR_ENABLE", String.valueOf(false)));
    private int port = Integer.parseInt(System.getenv().getOrDefault("TAPDATA_EF_MONITOR_PORT", String.valueOf(3003)));
    private int threadPoolSize = 4;

    private HttpServer server;

    public JdkHttpServerConfig(HealthEndpoint healthEndpoint,
                               PrometheusScrapeEndpoint prometheusScrapeEndpoint,
                               ObjectMapper objectMapper) {
        this.healthEndpoint = healthEndpoint;
        this.prometheusScrapeEndpoint = prometheusScrapeEndpoint;
        this.objectMapper = objectMapper;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        // 检查是否启用
        if (!enabled) {
            System.out.println("JDK Actuator HTTP server is disabled by configuration.");
            return;
        }

        try {
            // 创建HttpServer实例
            server = HttpServer.create(new InetSocketAddress(port), 0);

            // 配置线程池
            server.setExecutor(Executors.newFixedThreadPool(threadPoolSize));

            // 添加actuator端点
            server.createContext("/actuator/prometheus", new PrometheusHandler());
            server.createContext("/actuator/health", new HealthHandler());
            server.createContext("/actuator/info", new InfoHandler());
            server.createContext("/actuator", new ActuatorIndexHandler());

            // 启动服务器
            server.start();
            System.out.println("Actuator HTTP server started on port: " + port);
            System.out.println("Prometheus endpoint: http://localhost:" + port + "/actuator/prometheus");
            System.out.println("Health endpoint: http://localhost:" + port + "/actuator/health");

        } catch (IOException e) {
            System.err.println("Failed to start JDK HttpServer on port: " + port);
            e.printStackTrace();
        }
    }

    /**
     * 停止服务器的方法，可用于优雅关闭
     */
    public void stopServer() {
        if (server != null) {
            server.stop(0);
            System.out.println("Actuator HTTP server stopped");
        }
    }

    /**
     * Prometheus指标端点处理器
     */
    private class PrometheusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
                return;
            }

            try {
                // 调用官方端点获取Prometheus格式数据
                WebEndpointResponse<byte[]> endpointResponse = prometheusScrapeEndpoint.scrape(
                        PrometheusOutputFormat.CONTENT_TYPE_004,
                        null // 包含所有指标
                );

                byte[] responseBytes = endpointResponse.getBody();
                int statusCode = endpointResponse.getStatus();

                // 设置HTTP头
                exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
                exchange.sendResponseHeaders(statusCode, responseBytes.length);

                // 写入响应体
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(responseBytes);
                }

            } catch (Exception e) {
                System.err.println("Error generating Prometheus metrics: " + e.getMessage());
                String errorResponse = "# Error generating metrics: " + e.getMessage() + "\n";
                byte[] errorBytes = errorResponse.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
                exchange.sendResponseHeaders(500, errorBytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(errorBytes);
                }
            }
        }
    }

    /**
     * 健康检查端点处理器
     */
    private class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            try {
                // 调用官方健康端点
                SystemHealth health = (SystemHealth) healthEndpoint.health();
                String response = objectMapper.writeValueAsString(health);

                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
                exchange.sendResponseHeaders(200, response.getBytes(StandardCharsets.UTF_8).length);

                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes(StandardCharsets.UTF_8));
                }

            } catch (Exception e) {
                System.err.println("Error generating health check: " + e.getMessage());
                String errorResponse = "{\"status\":\"DOWN\",\"error\":\"" + e.getMessage() + "\"}";
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(500, errorResponse.getBytes(StandardCharsets.UTF_8).length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(errorResponse.getBytes(StandardCharsets.UTF_8));
                }
            }
        }
    }

    /**
     * 应用信息端点处理器
     */
    private class InfoHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            String response = "{\"app\":{\"name\":\"tapdata-application\",\"description\":\"TapData Application\",\"version\":\"1.0.0\"}}";

            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.getBytes(StandardCharsets.UTF_8).length);

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    /**
     * Actuator索引端点处理器
     */
    private class ActuatorIndexHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            String response = """
                    {
                      "_links": {
                        "self": {
                          "href": "http://localhost:%d/actuator",
                          "templated": false
                        },
                        "health": {
                          "href": "http://localhost:%d/actuator/health",
                          "templated": false
                        },
                        "info": {
                          "href": "http://localhost:%d/actuator/info",
                          "templated": false
                        },
                        "prometheus": {
                          "href": "http://localhost:%d/actuator/prometheus",
                          "templated": false
                        }
                      }
                    }
                    """;
            response = String.format(response, port, port, port, port);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.getBytes(StandardCharsets.UTF_8).length);

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

}