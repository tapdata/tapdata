package io.tapdata.pdk.core.tapnode;

import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class TapNodeInfo implements MemoryFetcher {
    private TapNodeSpecification tapNodeSpecification;

    public static final String NODE_TYPE_SOURCE = "Source";
    public static final String NODE_TYPE_TARGET = "Target";
    public static final String NODE_TYPE_SOURCE_TARGET = "SourceAndTarget";
    public static final String NODE_TYPE_PROCESSOR = "Processor";
    private String nodeType;
    private Class<?> nodeClass;

    public InputStream readResource(String pathInJar) {
        URL url = nodeClass.getClassLoader().getResource(pathInJar);
        if(url != null) {
            try {
                return url.openStream();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public Class<?> getNodeClass() {
        return nodeClass;
    }

    public void setNodeClass(Class<?> nodeClass) {
        this.nodeClass = nodeClass;
    }

    public TapNodeSpecification getTapNodeSpecification() {
        return tapNodeSpecification;
    }

    public void setTapNodeSpecification(TapNodeSpecification tapNodeSpecification) {
        this.tapNodeSpecification = tapNodeSpecification;
    }

    public String getNodeType() {
        return nodeType;
    }

    public void setNodeType(String nodeType) {
        this.nodeType = nodeType;
    }

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        return DataMap.create().kv("nodeType", nodeType).kv("nodeClass", nodeClass);
    }
}
