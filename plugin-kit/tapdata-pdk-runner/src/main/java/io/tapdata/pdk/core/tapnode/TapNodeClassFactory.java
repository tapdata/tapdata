package io.tapdata.pdk.core.tapnode;

import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.TapNode;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.classloader.DependencyURLClassLoader;
import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.core.connector.TapConnector;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.entity.reflection.ClassAnnotationHandler;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handle TapNode classes which annotated with OpenAPIConnector, OpenAPIProcessor, DatabaseConnector and DatabaseProcessor
 * <p>
 * Will be created for every URIClassloader which load the SDK jar
 */
public class TapNodeClassFactory implements MemoryFetcher {
    private static final String TAG = TapNodeClassFactory.class.getSimpleName();

    private volatile ClassAnnotationHandler[] classAnnotationHandlers;
    private final TapProcessorAnnotationHandler tapProcessorAnnotationHandler = new TapProcessorAnnotationHandler();
    private final TapConnectorAnnotationHandler tapConnectorAnnotationHandler = new TapConnectorAnnotationHandler();

    private final Map<String, TapNodeInstance> associateIdTapNodeIdMap = new ConcurrentHashMap<>();

    private ClassLoader classLoader;

    public TapNodeClassFactory() {

    }

    public boolean hasTapConnectorNodeId(String pdkId, String group, String version) {
        TapNodeInfo info = tapConnectorAnnotationHandler.getTapNodeInfo(pdkId, group, version);
        return info != null;
    }

    public boolean hasTapProcessorNodeId(String pdkId, String group, String version) {
        TapNodeInfo info = tapProcessorAnnotationHandler.getTapNodeInfo(pdkId, group, version);
        return info != null;
    }

    public TapNodeInstance createTapConnector(String associateId, String pdkId, String group, String version) {
        TapNodeInfo tapNodeInfo = tapConnectorAnnotationHandler.getTapNodeInfo(pdkId, group, version);
        TapNodeInstance instance = create(tapNodeInfo, associateId, pdkId);
        if(instance != null) {
            return instance;
        }
        throw new CoreException(PDKRunnerErrorCodes.NODE_CREATE_CONNECTOR_NOT_EXISTS, "Connector TapNodeId " + pdkId + " not found for associateId " + associateId);
    }

    public TapNodeInstance createTapProcessor(String associateId, String tapNodeId, String group, String version) {
        TapNodeInfo tapNodeInfo = tapProcessorAnnotationHandler.getTapNodeInfo(tapNodeId, group, version);
        TapNodeInstance instance = create(tapNodeInfo, associateId, tapNodeId);
        if(instance != null) {
            return instance;
        }
        throw new CoreException(PDKRunnerErrorCodes.NODE_CREATE_PROCESSOR_NOT_EXISTS, "Processor TapNodeId " + tapNodeId + " not found for associateId " + associateId);
    }

    private TapNodeInstance create(TapNodeInfo tapNodeInfo, String associateId, String tapNodeId) {
        if(tapNodeInfo != null) {
            Class<?> nodeClass = tapNodeInfo.getNodeClass();
            if(nodeClass != null) {
                if(!associateIdTapNodeIdMap.containsKey(associateId)) {
                    synchronized (this) {
                        if(!associateIdTapNodeIdMap.containsKey(associateId)) {
                            try {
                                Object instance = nodeClass.getConstructor().newInstance();
                                TapNodeInstance tapNodeInstance = new TapNodeInstance();
                                tapNodeInstance.setTapNodeInfo(tapNodeInfo);
                                tapNodeInstance.setTapNode((TapNode) instance);
                                associateIdTapNodeIdMap.put(associateId, tapNodeInstance);
                                return tapNodeInstance;
                            } catch (Throwable e) {
                                throw new CoreException(PDKRunnerErrorCodes.NODE_CREATE_OPENAPI_CONNECTOR, "New instance node class " + nodeClass + " tapNodeId " +  tapNodeId + " associateId " + associateId + " failed, " + e.getMessage());
                            }
                        }
                    }
                } else {
                    TapLogger.error(TAG, "{} is associated on id {} already, maybe forget to release associateId? ", tapNodeInfo.getNodeClass(), associateId);
                }
            }
        }
        return null;
    }

    public boolean isAssociateIdEmpty() {
        return associateIdTapNodeIdMap.isEmpty();
    }

    public boolean isAssociated(String associateId) {
        if(associateId == null)
            return false;
        return associateIdTapNodeIdMap.containsKey(associateId);
    }

    public String releaseAssociateId(String associateId) {
        if(associateId == null)
            return null;
        TapNodeInstance instance = associateIdTapNodeIdMap.remove(associateId);
        if(instance != null && instance.getTapNodeInfo() != null && instance.getTapNodeInfo().getTapNodeSpecification() != null) {
            String id = instance.getTapNodeInfo().getTapNodeSpecification().getId();
//            if(instance.getTapNode() != null) {
//                CommonUtils.ignoreAnyError(() -> instance.getTapNode().destroy(), TAG);
//            }
            return id;
        }
        return null;
    }

    public Collection<String> associatingIds() {
        return associateIdTapNodeIdMap.keySet();
    }

    public void clearAssociateIds() {
        associateIdTapNodeIdMap.clear();
    }

    public ClassAnnotationHandler[] getClassAnnotationHandlers() {
        if(classAnnotationHandlers == null) {
            synchronized (this) {
                if(classAnnotationHandlers == null) {
                    classAnnotationHandlers = new ClassAnnotationHandler[] {
                            tapProcessorAnnotationHandler,
                            tapConnectorAnnotationHandler
                    };
                }
            }
        }
        return classAnnotationHandlers;
    }

    public void applyNewClassloader(ClassLoader classLoader) {
        ClassLoader previousClassloader = this.classLoader;
        this.classLoader = classLoader;
        if(classAnnotationHandlers != null) {
            for(ClassAnnotationHandler classAnnotationHandler : classAnnotationHandlers) {
                if(classAnnotationHandler instanceof TapBaseAnnotationHandler) {
                    TapBaseAnnotationHandler tapBaseAnnotationHandler = (TapBaseAnnotationHandler) classAnnotationHandler;
                    tapBaseAnnotationHandler.applyNewerNotInfoMap();
                }
            }
        }
        releaseClassloader(previousClassloader);
    }

    private void releaseClassloader(ClassLoader classLoader) {
        if(classLoader == null)
            return;
        if(classLoader instanceof DependencyURLClassLoader) {
            DependencyURLClassLoader dependencyURLClassLoader = (DependencyURLClassLoader) classLoader;
            dependencyURLClassLoader.close();
        } else if(classLoader instanceof URLClassLoader) {
            URLClassLoader urlClassLoader = (URLClassLoader) classLoader;
            CommonUtils.ignoreAnyError(urlClassLoader::close, TAG);
        }

    }

    public TapNodeInfo getTapNodeInfoForConnector(String pdkId, String group, String version) {
        return tapConnectorAnnotationHandler.getTapNodeInfo(pdkId, group, version);
    }

    public Collection<TapNodeInfo> getConnectorTapNodeInfos() {
        return tapConnectorAnnotationHandler.getTapNodeInfos();
    }

    public TapNodeInfo getTapNodeInfoForProcessor(String id, String group, String version) {
        return tapProcessorAnnotationHandler.getTapNodeInfo(id, group, version);
    }

    public Collection<TapNodeInfo> getProcessorTapNodeInfos() {
        return tapProcessorAnnotationHandler.getTapNodeInfos();
    }

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        DataMap idGroupTapNodeInfoMap = DataMap.create().keyRegex(keyRegex);
        DataMap associateIdTapNodeIdMap = DataMap.create().keyRegex(keyRegex);
        DataMap dataMap = DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/
                .kv("idGroupTapNodeInfoMap", idGroupTapNodeInfoMap)
                .kv("associateIdTapNodeIdMap", associateIdTapNodeIdMap);
        if(classLoader instanceof DependencyURLClassLoader) {
            dataMap.kv("manifest", ((DependencyURLClassLoader) classLoader).manifest());
        }
        for(Map.Entry<String, TapNodeInfo> entry : tapConnectorAnnotationHandler.idGroupTapNodeInfoMap.entrySet()) {
            idGroupTapNodeInfoMap.kv(entry.getKey(), entry.getValue().memory(keyRegex, memoryLevel));
        }

        for(Map.Entry<String, TapNodeInstance> entry : this.associateIdTapNodeIdMap.entrySet()) {
            associateIdTapNodeIdMap.kv(entry.getKey(), entry.getValue().getTapNodeInfo().getTapNodeSpecification().idAndGroup());
        }
        return dataMap;
    }
}
