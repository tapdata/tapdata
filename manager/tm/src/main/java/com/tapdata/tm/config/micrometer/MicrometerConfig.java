package com.tapdata.tm.config.micrometer;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.actuate.metrics.web.servlet.WebMvcTagsProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * @author Dexter
 */
@Configuration
@Slf4j
public class MicrometerConfig {

    @Bean
    public WebMvcTagsProvider webMvcTagsProvider() {
        return new CustomWebMvcTagsProvider();
    }

    @Bean
    MeterRegistryCustomizer<MeterRegistry> metricsCommonTags() {
        return registry -> {
            registry.config().meterFilter(
                    new MeterFilter() {
                        @Override
                        public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
                            if (id.getType() == Meter.Type.TIMER&&id.getName().matches("^(http|hystrix){1}.*")) {
                                return DistributionStatisticConfig.builder()
                                        .sla(
                                                Duration.ofMillis(100).toNanos(),
                                                Duration.ofMillis(300).toNanos(),
                                                Duration.ofMillis(500).toNanos(),
                                                Duration.ofMillis(800).toNanos(),
                                                Duration.ofSeconds(1).toNanos(),
                                                Duration.ofSeconds(5).toNanos()
                                        )
                                        .minimumExpectedValue(Duration.ofMillis(1).toNanos())
                                        .maximumExpectedValue(Duration.ofSeconds(5).toNanos())
                                        .build()
                                        .merge(config);
                            } else {
                                return config;
                            }
                        }
                    });
        };
    }
}
