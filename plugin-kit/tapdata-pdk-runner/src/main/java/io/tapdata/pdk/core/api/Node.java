package io.tapdata.pdk.core.api;

import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.List;
import java.util.Map;


public abstract class Node {
    String dagId;
    String associateId;
    TapNodeInfo tapNodeInfo;
    List<Map<String, Object>> tasks;

    public void applyClassLoaderContext(Runnable runnable) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if(tapNodeInfo != null && tapNodeInfo.getNodeClass() != null) {
            Thread.currentThread().setContextClassLoader(tapNodeInfo.getNodeClass().getClassLoader());
        }
        try {
            runnable.run();
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
    }

    public ClassLoader getConnectorClassLoader() {
        return tapNodeInfo.getNodeClass().getClassLoader();
    }

    public String getDagId() {
        return dagId;
    }

    public String getAssociateId() {
        return associateId;
    }

    public TapNodeInfo getTapNodeInfo() {
        return tapNodeInfo;
    }

    public List<Map<String, Object>> getTasks() {
        return tasks;
    }
}
