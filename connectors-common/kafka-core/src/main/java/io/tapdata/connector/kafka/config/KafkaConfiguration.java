package io.tapdata.connector.kafka.config;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public interface KafkaConfiguration {

    Map<String, Object> build();

    Set<String> getRawTopics();

    Pattern getPatternTopics();

    boolean hasRawTopics();

    Duration getPollTimeout();

    boolean isIgnoreInvalidRecord();

    KafkaConfig getConfig();

}
