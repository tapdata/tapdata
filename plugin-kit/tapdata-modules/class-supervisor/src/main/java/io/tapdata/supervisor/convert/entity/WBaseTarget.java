package io.tapdata.supervisor.convert.entity;

import java.util.Map;
import java.util.Objects;

class WBaseTarget extends WBase implements Resolvable<WBaseTarget> {
    protected String type;
    protected String path;
    protected boolean needCreate;
    protected String scanPackage;
    protected String saveTo;
    protected String[] classPathCache;

    @Override
    public WBaseTarget parser(Map<String, Object> parserMap) {
        super.parser(parserMap);
        if (this.ignore) return null;
        this.type = WZTags.toString(parserMap, WZTags.W_TYPE, WZTags.DEFAULT_EMPTY);
        this.path = WZTags.toString(parserMap, WZTags.W_PATH, WZTags.DEFAULT_EMPTY);
        this.needCreate = WZTags.toBoolean(parserMap, WZTags.W_IS_CREATE, Boolean.FALSE);
        this.scanPackage = WZTags.toString(parserMap, WZTags.W_SCAN_PACKAGE, WZTags.DEFAULT_EMPTY);
        this.saveTo = WZTags.toString(parserMap, WZTags.W_SAVE_TO, WZTags.DEFAULT_EMPTY);
        WBaseTarget target;
        switch (this.type) {
            case WZTags.TYPE_EXTENDS:
                target = new WTypeExtends();
                break;
            case WZTags.W_PATH:
                target = new WTypePath();
                break;
            default:
                target = new WTypeName();
        }
        this.parser(target);
        return target;
    }

    public void parser(WBaseTarget target) {
        target.setType(this.type);
        target.setPath(this.path);
        target.setNeedCreate(this.needCreate);
        target.setScanPackage(this.scanPackage);
        target.setSaveTo(this.saveTo);
    }

    public String[] paths() {
        if (Objects.isNull(this.classPathCache)) {
            this.classPathCache = this.freshenPaths();
        }
        return this.classPathCache;
    }

    public String[] freshenPaths() {
        return new String[0];
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public boolean isNeedCreate() {
        return needCreate;
    }

    public void setNeedCreate(boolean needCreate) {
        this.needCreate = needCreate;
    }

    public String getScanPackage() {
        return scanPackage;
    }

    public void setScanPackage(String scanPackage) {
        this.scanPackage = scanPackage;
    }

    public String getSaveTo() {
        return saveTo;
    }

    public void setSaveTo(String saveTo) {
        this.saveTo = saveTo;
    }
}
