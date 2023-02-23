package io.tapdata.common.postman.entity.params;

import io.tapdata.common.postman.entity.params.body.*;
import io.tapdata.common.postman.enums.PostParam;

import java.util.*;

public abstract class Body<T> {
    protected String mode;
    protected T raw;
    protected Map<String, Object> options;
    protected String contentType = "application/json";
    public abstract Body<T> copyOne();
    public static Body createNoOne(){
        return new NoOne();
    }

    public static Body create(Map<String, Object> map) {
        if (Objects.nonNull(map) && !map.isEmpty()) {
            try {
                String mode = (String) map.get(PostParam.MODE);
                Map<String, Object> options = (Map<String, Object>) map.get(PostParam.OPTIONS);
                Class<? extends Body> bodyMode = ModeType.getByMode(mode);
                Body<?> body = bodyMode.newInstance();
                return body.mode(mode).options(options).autoSupplementData(map);
            } catch (Exception e) {
            }
        }
        return new NoOne();
    }

    public String mode() {
        return this.mode;
    }

    public Body<T> mode(String mode) {
        this.mode = mode;
        return this;
    }

    public T raw() {
        return this.raw;
    }

    public Body<T> raw(T raw) {
        this.raw = raw;
        return this;
    }

    public abstract Body<T> autoSupplementData(Map<String,Object> bodyMap);

    public Map<String, Object> options() {
        return this.options;
    }

    public Body<T> options(Map<String, Object> options) {
        this.options = options;
        return this;
    }

    public abstract Body<T> variableAssignment(Map<String, Object> param);

    public String contentType(){
        return this.contentType;
    }

    public abstract String bodyJson(Map<String,Object> appendMap);

    public abstract Body<T> setContentType();

    public void cleanCache(){

    }
}


