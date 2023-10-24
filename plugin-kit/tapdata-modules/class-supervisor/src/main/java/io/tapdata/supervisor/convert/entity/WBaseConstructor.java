package io.tapdata.supervisor.convert.entity;

import java.util.*;

class WBaseConstructor extends WBase implements Resolvable<WBaseConstructor> {
    private boolean needCreate;
    private List<String> args;

    public boolean isScanAllConstructor() {
        return scanAllConstructor;
    }

    private List<WCode> codes;
    private String returnType;
    private boolean scanAllConstructor;

    @Override
    public WBaseConstructor parser(Map<String, Object> parserMap) {
        super.parser(parserMap);
        if (this.ignore) return null;
        this.needCreate = WZTags.toBoolean(parserMap, WZTags.W_IS_CREATE, Boolean.FALSE);
        Object collection = WZTags.toObject(parserMap, WZTags.W_ARGS, new ArrayList<>());
        try {
            this.args = (List<String>) collection;
        } catch (Exception e) {
            this.scanAllConstructor = collection instanceof String && "*".equals(((String) collection).trim());
            this.args = new ArrayList<>();
        }
        this.returnType = WZTags.toString(parserMap, WZTags.W_RETURN_TYPE, WZTags.DEFAULT_VOID);
        this.codes = new ArrayList<>();
        try {
            List<Map<String, Object>> mapList = (List<Map<String, Object>>) WZTags.toList(parserMap, WZTags.W_CODE);
            for (Map<String, Object> objectMap : mapList) {
                if (Objects.nonNull(objectMap) && !objectMap.isEmpty()) {
                    Optional.ofNullable(new WCode().parser(objectMap)).ifPresent(this.codes::add);
                }
            }
        } catch (Exception ignored) {
        }
        return this;
    }

    public boolean isNeedCreate() {
        return needCreate;
    }

    public void setNeedCreate(boolean needCreate) {
        this.needCreate = needCreate;
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
