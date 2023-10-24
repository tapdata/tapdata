package io.tapdata.supervisor.convert.entity;

import java.util.*;

class WBaseMethod extends WBase implements Resolvable<WBaseMethod>{
    private boolean needCreate;
    private String name;
    private List<String> args;
    private List<WCode> codes;
    private String createWith;
    private String returnType;

    public String getCreateWith() {
        return createWith;
    }

    @Override
    public WBaseMethod parser(Map<String, Object> parserMap) {
        super.parser(parserMap);
        if (this.ignore) return null;
        this.name = WZTags.toString(parserMap, WZTags.W_NAME, WZTags.DEFAULT_EMPTY);
        this.needCreate = WZTags.toBoolean(parserMap, WZTags.W_IS_CREATE, Boolean.FALSE);
        this.createWith = WZTags.toString(parserMap,WZTags.CREATE_WITH,WZTags.DEFAULT_EMPTY);
        try {
            this.args = (List<String>) WZTags.toList(parserMap,WZTags.W_ARGS);
        }catch (Exception e){
            this.args = new ArrayList<>();
        }
        this.returnType = WZTags.toString(parserMap, WZTags.W_RETURN_TYPE, WZTags.DEFAULT_VOID);
        this.codes = new ArrayList<>();
        try {
            List<Map<String, Object>> mapList = (List<Map<String, Object>>) WZTags.toList(parserMap, WZTags.W_CODE);
            for (Map<String, Object> objectMap : mapList) {
                if (Objects.nonNull(objectMap) && !objectMap.isEmpty()) {
                    Optional.ofNullable((new WCode().parser(objectMap))).ifPresent(this.codes::add);
                }
            }
        }catch (Exception ignored){
        }
        return this;
    }

    public boolean isNeedCreate() {
        return needCreate;
    }

    public void setNeedCreate(boolean needCreate) {
        this.needCreate = needCreate;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getArgs() {
        return args;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }

    public List<WCode> getCodes() {
        return codes;
    }

    public void setCodes(List<WCode> codes) {
        this.codes = codes;
    }

    public String getReturnType() {
        return returnType;
    }

    public void setReturnType(String returnType) {
        this.returnType = returnType;
    }
}
