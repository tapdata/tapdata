package io.tapdata.pdk.core.api;

import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.List;
import java.util.Map;


public abstract class Node {
    String dagId;
    String associateId;
    TapNodeInfo tapNodeInfo;
    List<Map<String, Object>> tasks;

    public String id() {
        return this.getClass().getSimpleName() + "_" + tapNodeInfo.getTapNodeSpecification().idAndGroup();
    }

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

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(this.getClass().getSimpleName()).append(": ");
        TapNodeInfo nodeInfo = this.getTapNodeInfo();
        TapNodeSpecification specification = null;
        if(nodeInfo != null && (specification = nodeInfo.getTapNodeSpecification()) != null) {
            builder.append("id=").append(specification.getId()).append("; ");
            builder.append("group=").append(specification.getGroup()).append("; ");
            builder.append("version=").append(specification.getVersion()).append("; ");
            builder.append("nodeType=").append(nodeInfo.getNodeType()).append("; ");
            builder.append("nodeClass=").append(nodeInfo.getNodeClass()).append("; ");
        }
        builder.append("associateId=").append(associateId).append("; ");
        builder.append("dagId=").append(dagId).append(".");
        return builder.toString();
    }
}
