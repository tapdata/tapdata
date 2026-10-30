package com.tapdata.tm.actuator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.data.elasticsearch.autoconfigure.DataElasticsearchAutoConfiguration;
import org.springframework.boot.data.elasticsearch.autoconfigure.health.DataElasticsearchReactiveHealthContributorAutoConfiguration;
import org.springframework.boot.data.elasticsearch.health.DataElasticsearchReactiveHealthIndicator;
import org.springframework.boot.elasticsearch.autoconfigure.ElasticsearchClientAutoConfiguration;
import org.springframework.boot.elasticsearch.autoconfigure.ElasticsearchRestClientAutoConfiguration;
import org.springframework.boot.elasticsearch.autoconfigure.health.ElasticsearchRestHealthContributorAutoConfiguration;
import org.springframework.boot.elasticsearch.health.ElasticsearchRestClientHealthIndicator;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.elasticsearch.client.elc.ReactiveElasticsearchClient;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TAP-12289：加载生产默认配置与真实 Elasticsearch 自动配置，验证可选健康检查的开关。
 * 不连接外部 ES，也不关闭全部健康检查；端点暴露由 ActuatorExposureIT 单独覆盖。
 */
class ElasticsearchHealthIT {
    private ApplicationContextRunner context(boolean reactive) throws java.io.IOException {
        var sources = new YamlPropertySourceLoader()
                .load("tm-default", new ClassPathResource("application-default.yml"));
        return new ApplicationContextRunner()
                .withInitializer(application -> sources.forEach(source ->
                        application.getEnvironment().getPropertySources().addLast(source)))
                .withConfiguration(AutoConfigurations.of(
                        ElasticsearchRestClientAutoConfiguration.class,
                        ElasticsearchClientAutoConfiguration.class,
                        reactive ? DataElasticsearchAutoConfiguration.class
                                : ElasticsearchRestHealthContributorAutoConfiguration.class,
                        reactive ? DataElasticsearchReactiveHealthContributorAutoConfiguration.class
                                : ElasticsearchRestHealthContributorAutoConfiguration.class));
    }

    @Test
    @DisplayName("TM 默认配置：不注册同步 ES 健康检查")
    void should_disable_rest_health_by_default() throws Exception {
        context(false).run(application -> {
            assertThat(application).hasNotFailed();
            assertThat(application).hasBean("elasticsearchRestClient");
            assertThat(application).doesNotHaveBean(ElasticsearchRestClientHealthIndicator.class);
        });
    }

    @Test
    @DisplayName("TM 默认配置：不注册响应式 ES 健康检查")
    void should_disable_reactive_health_by_default() throws Exception {
        context(true).run(application -> {
            assertThat(application).hasNotFailed();
            assertThat(application).hasSingleBean(ReactiveElasticsearchClient.class);
            assertThat(application).doesNotHaveBean(DataElasticsearchReactiveHealthIndicator.class);
        });
    }

    @Test
    @DisplayName("显式启用：保留同步 ES 健康检查")
    void should_allow_explicit_rest_health() throws Exception {
        context(false).withPropertyValues("management.health.elasticsearch.enabled=true")
                .run(application -> {
                    assertThat(application).hasNotFailed();
                    assertThat(application).hasSingleBean(ElasticsearchRestClientHealthIndicator.class);
                });
    }

    @Test
    @DisplayName("显式启用：保留响应式 ES 健康检查")
    void should_allow_explicit_reactive_health() throws Exception {
        context(true).withPropertyValues("management.health.elasticsearch.enabled=true")
                .run(application -> {
                    assertThat(application).hasNotFailed();
                    assertThat(application).hasSingleBean(DataElasticsearchReactiveHealthIndicator.class);
                });
    }
}
