package io.tapdata.common;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.kit.EmptyKit;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FileConfig implements Serializable {

    private static final String TAG = FileConfig.class.getSimpleName();
    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class); //json util
    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util

    private String fileType;
    private String protocol;
    private String fileEncoding;
    private String filePathString;
    private Set<String> filePathSet;
    private String includeRegString;
    private Set<String> includeRegs;
    private String excludeRegString;
    private Set<String> excludeRegs;
    private Boolean recursive;
    private String modelName;
    private Boolean justString;
    private int headerLine;
    private String header;
    private int dataStartLine;
    private String writeFilePath;
    private String fileNameExpression;

    public FileConfig load(Map<String, Object> map) {
        assert beanUtils != null;
        beanUtils.mapToBean(map, this);
        if (EmptyKit.isNotBlank(filePathString)) {
            filePathSet = Arrays.stream(filePathString.split(",")).collect(Collectors.toSet());
        } else {
            filePathSet = Collections.singleton("");
        }
        if (EmptyKit.isNotBlank(includeRegString)) {
            includeRegs = Arrays.stream(includeRegString.split(",")).collect(Collectors.toSet());
        }
        if (EmptyKit.isNotBlank(excludeRegString)) {
            excludeRegs = Arrays.stream(excludeRegString.split(",")).collect(Collectors.toSet());
        }
        if (headerLine < 0) {
            headerLine = 0;
        }
        if (dataStartLine < headerLine + 1) {
            dataStartLine = headerLine + 1;
        }
        return this;
    }

    public FileConfig load(String json) {
        try {
            assert beanUtils != null;
            assert jsonParser != null;
            beanUtils.copyProperties(jsonParser.fromJson(json, this.getClass()), this);
            if (EmptyKit.isNotBlank(filePathString)) {
                filePathSet = Arrays.stream(filePathString.split(",")).collect(Collectors.toSet());
            }
            return this;
        } catch (Exception e) {
            TapLogger.error(TAG, "config json file is invalid!");
            e.printStackTrace();
        }
        return null;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getFileEncoding() {
        return fileEncoding;
    }

    public void setFileEncoding(String fileEncoding) {
        this.fileEncoding = fileEncoding;
    }

    public String getFilePathString() {
        return filePathString;
    }

    public void setFilePathString(String filePathString) {
        this.filePathString = filePathString;
    }

    public Set<String> getFilePathSet() {
        return filePathSet;
    }

    public void setFilePathSet(Set<String> filePathSet) {
        this.filePathSet = filePathSet;
    }

    public Boolean getRecursive() {
        return recursive;
    }

    public void setRecursive(Boolean recursive) {
        this.recursive = recursive;
    }

    public String getIncludeRegString() {
        return includeRegString;
    }

    public void setIncludeRegString(String includeRegString) {
        this.includeRegString = includeRegString;
    }

    public Set<String> getIncludeRegs() {
        return includeRegs;
    }

    public void setIncludeRegs(Set<String> includeRegs) {
        this.includeRegs = includeRegs;
    }

    public String getExcludeRegString() {
        return excludeRegString;
    }

    public void setExcludeRegString(String excludeRegString) {
        this.excludeRegString = excludeRegString;
    }

    public Set<String> getExcludeRegs() {
        return excludeRegs;
    }

    public void setExcludeRegs(Set<String> excludeRegs) {
        this.excludeRegs = excludeRegs;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public Boolean getJustString() {
        return justString;
    }

    public void setJustString(Boolean justString) {
        this.justString = justString;
    }

    public int getHeaderLine() {
        return headerLine;
    }

    public void setHeaderLine(int headerLine) {
        this.headerLine = headerLine;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public int getDataStartLine() {
        return dataStartLine;
    }

    public void setDataStartLine(int dataStartLine) {
        this.dataStartLine = dataStartLine;
    }

    public String getWriteFilePath() {
        return writeFilePath;
    }

    public void setWriteFilePath(String writeFilePath) {
        this.writeFilePath = writeFilePath;
    }

    public String getFileNameExpression() {
        return fileNameExpression;
    }

    public void setFileNameExpression(String fileNameExpression) {
        this.fileNameExpression = fileNameExpression;
    }
}
