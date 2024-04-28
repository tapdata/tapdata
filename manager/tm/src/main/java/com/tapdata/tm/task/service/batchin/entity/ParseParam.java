package com.tapdata.tm.task.service.batchin.entity;

import com.tapdata.tm.config.security.UserDetail;
import org.springframework.web.multipart.MultipartFile;

import java.util.Map;

public class ParseParam {
    MultipartFile multipartFile;
    String source;
    String sink;
    UserDetail user;

    String relMigStr;
    Map<String, Object> relMigInfo;

    public MultipartFile getMultipartFile() {
        return multipartFile;
    }

    public void setMultipartFile(MultipartFile multipartFile) {
        this.multipartFile = multipartFile;
    }

    public ParseParam withMultipartFile(MultipartFile multipartFile) {
        this.multipartFile = multipartFile;
        return this;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public ParseParam withSource(String source) {
        this.source = source;
        return this;
    }

    public String getSink() {
        return sink;
    }

    public void setSink(String sink) {
        this.sink = sink;
    }

    public ParseParam withSink(String sink) {
        this.sink = sink;
        return this;
    }

    public UserDetail getUser() {
        return user;
    }

    public void setUser(UserDetail user) {
        this.user = user;
    }

    public ParseParam withUser(UserDetail user) {
        this.user = user;
        return this;
    }

    public String getRelMigStr() {
        return relMigStr;
    }

    public void setRelMigStr(String relMigStr) {
        this.relMigStr = relMigStr;
    }

    public Map<String, Object> getRelMigInfo() {
        return relMigInfo;
    }

    public void setRelMigInfo(Map<String, Object> relMigInfo) {
        this.relMigInfo = relMigInfo;
    }
}
