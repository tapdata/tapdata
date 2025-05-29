package com.tapdata.tm.config.micrometer;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.observation.ServerRequestObservationConvention;

import java.time.Duration;
import java.util.List;
import java.util.Collections;

/**
 * @author Dexter
 */
@Configuration
@Slf4j
public class MicrometerConfig {

    @Bean
    public ServerRequestObservationConvention serverRequestObservationConvention(
            @Autowired(required = false) List<CustomServerRequestObservationConvention.CustomTagsContributor> contributors) {
        return new CustomServerRequestObservationConvention(false,
                contributors != null ? contributors : Collections.emptyList());
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
