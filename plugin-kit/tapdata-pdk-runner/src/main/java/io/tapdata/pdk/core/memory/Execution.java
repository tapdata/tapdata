package io.tapdata.pdk.core.memory;

import io.tapdata.entity.memory.MemoryFetcher;

import java.util.List;
public class Execution {
    public static final String OUTPUT_TYPE_TAP_LOGGER = "TapLogger";
    public static final String OUTPUT_TYPE_FILE = "File";
    private String outputType;

    private String outputFile;

    public final static String SCOPE_ALL = "all";
    public final static String SCOPE_CUSTOM = "custom";
    private String scope;
    private List<String> customScopes;

    private String keyRegex;
    public static final String MEMORY_LEVEL_SUMMARY = MemoryFetcher.MEMORY_LEVEL_SUMMARY;
    public static final String MEMORY_LEVEL_IN_DETAIL = MemoryFetcher.MEMORY_LEVEL_SUMMARY;
    private String memoryLevel;

    private Integer expectIntervalSeconds;

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    public List<String> getCustomScopes() {
        return customScopes;
    }

    public void setCustomScopes(List<String> customScopes) {
        this.customScopes = customScopes;
    }

    public Integer getExpectIntervalSeconds() {
        return expectIntervalSeconds;
    }

    public void setExpectIntervalSeconds(Integer expectIntervalSeconds) {
        this.expectIntervalSeconds = expectIntervalSeconds;
    }

    public String getOutputType() {
        return outputType;
    }

    public void setOutputType(String outputType) {
        this.outputType = outputType;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

    public String getMemoryLevel() {
        return memoryLevel;
    }

    public void setMemoryLevel(String memoryLevel) {
        this.memoryLevel = memoryLevel;
    }

    public String getKeyRegex() {
        return keyRegex;
    }

    public void setKeyRegex(String keyRegex) {
        this.keyRegex = keyRegex;
    }
}
