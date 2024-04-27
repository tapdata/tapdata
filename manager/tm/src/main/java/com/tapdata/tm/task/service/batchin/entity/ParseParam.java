package com.tapdata.tm.task.service.batchin.entity;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.batchin.dto.RelMigBaseDto;
import org.springframework.web.multipart.MultipartFile;

import java.util.Map;

public class ParseParam<Dto extends RelMigBaseDto> {
    MultipartFile multipartFile;
    String source;
    String sink;
    UserDetail user;

    Dto relMig;
    String relMigStr;
    Map<String, Object> relMigInfo;

    public MultipartFile getMultipartFile() {
        return multipartFile;
    }

    public void setMultipartFile(MultipartFile multipartFile) {
        this.multipartFile = multipartFile;
    }

    public ParseParam<Dto> withMultipartFile(MultipartFile multipartFile) {
        this.multipartFile = multipartFile;
        return this;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public ParseParam<Dto> withSource(String source) {
        this.source = source;
        return this;
    }

    public String getSink() {
        return sink;
    }

    public void setSink(String sink) {
        this.sink = sink;
    }

    public ParseParam<Dto> withSink(String sink) {
        this.sink = sink;
        return this;
    }

    public UserDetail getUser() {
        return user;
    }

    public void setUser(UserDetail user) {
        this.user = user;
    }

    public ParseParam<Dto> withUser(UserDetail user) {
        this.user = user;
        return this;
    }

    public Dto getRelMig() {
        return relMig;
    }

    public void setRelMig(Dto relMig) {
        this.relMig = relMig;
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
