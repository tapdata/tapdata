package io.tapdata.connector.custom.config;

import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;

import java.io.Serializable;
import java.util.Map;

public class CustomConfig implements Serializable {

    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util

    private String __connectionType; //add this system config to recognise
    private String collectionName;
    private String uniqueKeys;
    private String syncType = "initial_sync";
    private String jsEngineName = "graal.js"; //new version
    private Boolean customBeforeOpr;
    private String customBeforeScript = "";
    private String targetScript = "";
    private String cdcScript = "";
    private String historyScript = "";
    private Boolean customAfterOpr;
    private String customAfterScript = "";

    public CustomConfig load(Map<String, Object> map) {
        assert beanUtils != null;
        return beanUtils.mapToBean(map, this);
    }

    public String get__connectionType() {
        return __connectionType;
    }

    public void set__connectionType(String __connectionType) {
        this.__connectionType = __connectionType;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getUniqueKeys() {
        return uniqueKeys;
    }

    public void setUniqueKeys(String uniqueKeys) {
        this.uniqueKeys = uniqueKeys;
    }

    public String getSyncType() {
        return syncType;
    }

    public void setSyncType(String syncType) {
        this.syncType = syncType;
    }

    public String getJsEngineName() {
        return jsEngineName;
    }

    public void setJsEngineName(String jsEngineName) {
        this.jsEngineName = jsEngineName;
    }

    public Boolean getCustomBeforeOpr() {
        return customBeforeOpr;
    }

    public void setCustomBeforeOpr(Boolean customBeforeOpr) {
        this.customBeforeOpr = customBeforeOpr;
    }

    public String getCustomBeforeScript() {
        return customBeforeScript;
    }

    public void setCustomBeforeScript(String customBeforeScript) {
        this.customBeforeScript = customBeforeScript;
    }

    public String getTargetScript() {
        return targetScript;
    }

    public void setTargetScript(String targetScript) {
        this.targetScript = targetScript;
    }

    public String getCdcScript() {
        return cdcScript;
    }

    public void setCdcScript(String cdcScript) {
        this.cdcScript = cdcScript;
    }

    public String getHistoryScript() {
        return historyScript;
    }

    public void setHistoryScript(String historyScript) {
        this.historyScript = historyScript;
    }

    public Boolean getCustomAfterOpr() {
        return customAfterOpr;
    }

    public void setCustomAfterOpr(Boolean customAfterOpr) {
        this.customAfterOpr = customAfterOpr;
    }

    public String getCustomAfterScript() {
        return customAfterScript;
    }

    public void setCustomAfterScript(String customAfterScript) {
        this.customAfterScript = customAfterScript;
    }
}
