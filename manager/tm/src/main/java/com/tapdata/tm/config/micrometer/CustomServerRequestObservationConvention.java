package com.tapdata.tm.config.micrometer;

import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import org.springframework.http.server.observation.ServerRequestObservationContext;
import org.springframework.http.server.observation.ServerRequestObservationConvention;

import java.util.Collections;
import java.util.List;

public class CustomServerRequestObservationConvention implements ServerRequestObservationConvention {

    private final boolean ignoreTrailingSlash;
    private final List<CustomTagsContributor> contributors;

    public CustomServerRequestObservationConvention() {
        this(false);
    }

    public CustomServerRequestObservationConvention(List<CustomTagsContributor> contributors) {
        this(false, contributors);
    }

    public CustomServerRequestObservationConvention(boolean ignoreTrailingSlash) {
        this(ignoreTrailingSlash, Collections.emptyList());
    }

    public CustomServerRequestObservationConvention(boolean ignoreTrailingSlash,
                                                    List<CustomTagsContributor> contributors) {
        this.ignoreTrailingSlash = ignoreTrailingSlash;
        this.contributors = contributors;
    }

    @Override
    public String getName() {
        return "http.server.requests";
    }

    @Override
    public String getContextualName(ServerRequestObservationContext context) {
        return "http " + context.getCarrier().getMethod().toLowerCase();
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(ServerRequestObservationContext context) {
        KeyValues keyValues = KeyValues.of(
                // HTTP method
                KeyValue.of("method", context.getCarrier().getMethod()),
                // URI
                KeyValue.of("uri", extractUri(context)),
                // Status
                KeyValue.of("status", getStatusValue(context))
        );

        // 添加贡献者提供的标签
        for (CustomTagsContributor contributor : this.contributors) {
            keyValues = keyValues.and(contributor.getKeyValues(context));
        }

        return keyValues;
    }

    @Override
    public KeyValues getHighCardinalityKeyValues(ServerRequestObservationContext context) {
        return KeyValues.empty();
    }

    private String extractUri(ServerRequestObservationContext context) {
        String uri = context.getCarrier().getRequestURI();
        if (ignoreTrailingSlash && uri.length() > 1 && uri.endsWith("/")) {
            return uri.substring(0, uri.length() - 1);
        }
        return uri;
    }

    private String getStatusValue(ServerRequestObservationContext context) {
        return (context.getResponse() != null)
                ? String.valueOf(context.getResponse().getStatus())
                : "200";
    }

    // 自定义标签贡献者接口
    public interface CustomTagsContributor {
        KeyValues getKeyValues(ServerRequestObservationContext context);
    }
}