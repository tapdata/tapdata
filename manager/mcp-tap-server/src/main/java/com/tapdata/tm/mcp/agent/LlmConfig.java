package com.tapdata.tm.mcp.agent;

import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;

public class LlmConfig {

    private String baseUrl;
    private String apiKey;
    private String authToken;
    private String model;
    private Double temperature;
    private Integer maxTokens;

    public void validate() {
        if (StringUtils.isBlank(baseUrl)) {
            throw new IllegalArgumentException("llm.baseUrl is required");
        }
        if (StringUtils.isBlank(normalizedApiKey())) {
            throw new IllegalArgumentException("llm.apiKey is required");
        }
        if (StringUtils.isBlank(model)) {
            throw new IllegalArgumentException("llm.model is required");
        }
    }

    public String normalizedBaseUrl() {
        String value = StringUtils.trimToEmpty(baseUrl);
        while (value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        if (StringUtils.isBlank(value)) {
            return value;
        }
        try {
            URI uri = new URI(value);
            if (StringUtils.isBlank(uri.getPath())) {
                return new URI(uri.getScheme(), uri.getAuthority(), "/v1", null, null).toString();
            }
        } catch (URISyntaxException ignored) {
            return value;
        }
        return value;
    }

    public String normalizedApiKey() {
        return StringUtils.defaultIfBlank(StringUtils.trimToNull(apiKey), StringUtils.trimToNull(authToken));
    }

    public String normalizedModel() {
        return StringUtils.trimToEmpty(model);
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public String getAuthToken() {
        return authToken;
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    public Integer getMaxTokens() {
        return maxTokens;
    }

    public void setMaxTokens(Integer maxTokens) {
        this.maxTokens = maxTokens;
    }
}
