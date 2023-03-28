package io.tapdata.supervisor.convert.entity;

import io.tapdata.supervisor.utils.ClassUtil;

import java.util.Map;
import java.util.Objects;

class WBaseTarget extends WBase implements Resolvable<WBaseTarget> {
    protected String type;
    protected String path;
    protected boolean needCreate;
    protected String scanPackage;
    protected String saveTo;
    protected String[] classPathCache;
    protected String jarFilePath;
    protected ClassUtil classUtil;

    public ClassUtil getClassUtil() {
        return classUtil;
    }

    public void setClassUtil(ClassUtil classUtil) {
        this.classUtil = classUtil;
    }

    public WBaseTarget classUtil(ClassUtil classUtil) {
        this.classUtil = classUtil;
        return this;
    }
    public WBaseTarget(String savePath, String jarFilePath) {
        this.saveTo = savePath;
        this.jarFilePath = jarFilePath;
    }

    @Override
    public WBaseTarget parser(Map<String, Object> parserMap) {
        super.parser(parserMap);
        if (this.ignore) return null;
        this.type = WZTags.toString(parserMap, WZTags.W_TYPE, WZTags.DEFAULT_EMPTY);
        this.path = WZTags.toString(parserMap, WZTags.W_PATH, WZTags.DEFAULT_EMPTY);
        this.needCreate = WZTags.toBoolean(parserMap, WZTags.W_IS_CREATE, Boolean.FALSE);
        this.scanPackage = WZTags.toString(parserMap, WZTags.W_SCAN_PACKAGE, WZTags.DEFAULT_EMPTY);
        WBaseTarget target;
        switch (this.type) {
            case WZTags.TYPE_EXTENDS:
                target = new WTypeExtends(this.saveTo, this.jarFilePath);
                break;
            case WZTags.W_PATH:
                target = new WTypePath(this.saveTo, this.jarFilePath);
                break;
            default:
                target = new WTypeName(this.saveTo, this.jarFilePath);
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
        target.setClassUtil(this.classUtil);
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
