package io.tapdata.pdk.core.tapnode;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TapConnectorAnnotationHandler extends TapBaseAnnotationHandler {
    private static final String TAG = TapConnectorAnnotationHandler.class.getSimpleName();

    public TapConnectorAnnotationHandler() {
        super();
    }

    @Override
    public void handle(Set<Class<?>> classes) throws CoreException {
        if (classes != null && !classes.isEmpty()) {
            newerIdGroupTapNodeInfoMap = new ConcurrentHashMap<>();
            TapLogger.debug(TAG, "--------------TapConnector Classes Start------------- size {}", classes.size());
            for (Class<?> clazz : classes) {
                TapConnectorClass tapConnectorClass = clazz.getAnnotation(TapConnectorClass.class);
                if (tapConnectorClass != null) {
                    URL url = clazz.getClassLoader().getResource(tapConnectorClass.value());
                    if (url != null) {
                        TapNodeSpecification tapNodeSpecification = null;
                        try {
                            InputStream is = url.openStream();
                            String json = IOUtils.toString(is, StandardCharsets.UTF_8);
                            TapNodeContainer tapNodeContainer = InstanceFactory.instance(JsonParser.class).fromJson(json, TapNodeContainer.class);
                            tapNodeSpecification = tapNodeContainer.getProperties();

                            String errorMessage = null;
                            if (tapNodeSpecification == null) {
                                errorMessage = "Specification not found";
                            } else {
                                if(tapNodeSpecification.getGroup() == null) {
                                    tapNodeSpecification.setGroup(clazz.getPackage().getImplementationVendor());
                                }
                                if(tapNodeSpecification.getVersion() == null) {
                                    tapNodeSpecification.setVersion(clazz.getPackage().getImplementationVersion());
                                }
                            }

                            if (errorMessage == null)
                                errorMessage = tapNodeSpecification.verify();
                            if (errorMessage != null) {
                                TapLogger.warn(TAG, "Tap node specification is illegal, will be ignored, path {} content {} errorMessage {}", tapConnectorClass.value(), json, errorMessage);
                                continue;
                            }

                            tapNodeSpecification.setConfigOptions(tapNodeContainer.getConfigOptions());
                            if(tapNodeContainer.getDataTypes() != null) {
                                DefaultExpressionMatchingMap matchingMap = DefaultExpressionMatchingMap.map(tapNodeContainer.getDataTypes());
//                                matchingMap.setValueFilter(defaultMap -> {
//                                    TapMapping tapMapping = (TapMapping) defaultMap.get(TapMapping.FIELD_TYPE_MAPPING);
//                                    if(tapMapping == null) {
//                                        defaultMap.put(TapMapping.FIELD_TYPE_MAPPING, TapMapping.build(defaultMap));
//                                    }
//                                });
                                tapNodeSpecification.setDataTypesMap(matchingMap);
                            }
                            ClassLoader classLoader = clazz.getClassLoader();

                            try {
                                Method method = classLoader.getClass().getMethod("manifest");
                                method.setAccessible(true);
                                tapNodeSpecification.setManifest((Map<String, String>) method.invoke(classLoader));
                            } catch(Throwable throwable) {
                                TapLogger.debug(TAG, "Read manifest failed, " + throwable.getMessage());
                            }
//                            tapNodeSpecification.setManifest();
                            String connectorType = findConnectorType(clazz);
                            if (connectorType == null) {
                                TapLogger.error(TAG, "Connector class for id {} title {} only have TapConnector annotation, but not implement the necessary methods, {} will be ignored...", tapNodeSpecification.idAndGroup(), tapNodeSpecification.getName(), clazz);
                                continue;
                            }
                            TapNodeInfo tapNodeInfo = newerIdGroupTapNodeInfoMap.get(tapNodeSpecification.idAndGroup());
                            if (tapNodeInfo == null) {
                                tapNodeInfo = new TapNodeInfo();
                                tapNodeInfo.setTapNodeSpecification(tapNodeSpecification);
                                tapNodeInfo.setNodeType(connectorType);
                                tapNodeInfo.setNodeClass(clazz);
                                newerIdGroupTapNodeInfoMap.put(tapNodeSpecification.idAndGroup(), tapNodeInfo);
                                TapLogger.debug(TAG, "Found new connector {} type {}", tapNodeSpecification.idAndGroup(), connectorType);
                            } else {
                                TapNodeSpecification specification = tapNodeInfo.getTapNodeSpecification();
                                tapNodeInfo.setTapNodeSpecification(specification);
                                tapNodeInfo.setNodeType(connectorType);
                                tapNodeInfo.setNodeClass(clazz);
                                TapLogger.warn(TAG, "Found newer connector {} type {}", tapNodeSpecification.idAndGroup(), connectorType);
                            }
                        } catch (Throwable throwable) {
                            TapLogger.error(TAG, "Handle tap node specification failed, path {} error {}", tapConnectorClass.value(), throwable.getMessage());
                        }
                    } else {
                        TapLogger.error(TAG, "Resource {} doesn't be found, connector class {} will be ignored", tapConnectorClass.value(), clazz);
                    }
                }
            }
            TapLogger.debug(TAG, "--------------TapConnector Classes End-------------");
        }
    }

    private String findConnectorType(Class<?> clazz) {
        boolean isSource = false;
        boolean isTarget = false;
        if (TapConnector.class.isAssignableFrom(clazz)) {
            try {
                TapConnector connector = (TapConnector) clazz.getConstructor().newInstance();
                ConnectorFunctions connectorFunctions = new ConnectorFunctions();
                TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
                connector.registerCapabilities(connectorFunctions, codecRegistry);

                if (connectorFunctions.getBatchReadFunction() != null || connectorFunctions.getStreamReadFunction() != null) {
                    isSource = true;
                }
                if (connectorFunctions.getWriteRecordFunction() != null) {
                    isTarget = true;
                }
            } catch (Throwable e) {
                e.printStackTrace();
                TapLogger.error(TAG, "Find connector type failed, {} clazz {} will be ignored", e.getMessage(), clazz);
            }
        }

        if (isSource && isTarget) {
            return TapNodeInfo.NODE_TYPE_SOURCE_TARGET;
        } else if (isSource) {
            return TapNodeInfo.NODE_TYPE_SOURCE;
        } else if (isTarget) {
            return TapNodeInfo.NODE_TYPE_TARGET;
        }
        return null;
    }

    @Override
    public Class<? extends Annotation> watchAnnotation() {
        return TapConnectorClass.class;
    }

}
