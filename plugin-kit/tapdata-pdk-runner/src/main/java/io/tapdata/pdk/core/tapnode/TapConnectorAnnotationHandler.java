package io.tapdata.pdk.core.tapnode;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.ReflectionUtil;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarFile;

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
            Set<String> connectorSupperClassNames = new HashSet<>();
            for (Class<?> aClass : classes) {
                connectorSupperClassNames.addAll(findAllConnectorSuperClassName(aClass));
            }

            for (Class<?> clazz : classes) {
                TapConnectorClass tapConnectorClass = clazz.getAnnotation(TapConnectorClass.class);
                if (tapConnectorClass != null) {
                    if (connectorSupperClassNames.contains(clazz.getCanonicalName())) {
                        continue;
                    }

                    URL url = clazz.getClassLoader().getResource(tapConnectorClass.value());
                    if (url != null) {
                        String filePath = (Optional.ofNullable(url.getPath()).orElse("")).replace("!/" + tapConnectorClass.value(), "");
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
                                if (tapNodeSpecification.getGroup() == null) {
                                    tapNodeSpecification.setGroup(clazz.getPackage().getImplementationVendor());
                                }
                                if (tapNodeSpecification.getVersion() == null) {
                                    tapNodeSpecification.setVersion(clazz.getPackage().getImplementationVersion());
                                }
                            }

                            if (errorMessage == null)
                                errorMessage = tapNodeSpecification.verify();
                            if (errorMessage != null) {
                                TapLogger.warn(TAG, "Tap node specification is illegal, will be ignored, path {} content {} errorMessage {}, file path is: {}", tapConnectorClass.value(), json, errorMessage, filePath);
                                continue;
                            }

                            tapNodeSpecification.setConfigOptions(tapNodeContainer.getConfigOptions());
                            if (tapNodeContainer.getDataTypes() != null) {
                                DefaultExpressionMatchingMap matchingMap = DefaultExpressionMatchingMap.map(tapNodeContainer.getDataTypes());
//                                matchingMap.setValueFilter(defaultMap -> {
//                                    TapMapping tapMapping = (TapMapping) defaultMap.get(TapMapping.FIELD_TYPE_MAPPING);
//                                    if(tapMapping == null) {
//                                        defaultMap.put(TapMapping.FIELD_TYPE_MAPPING, TapMapping.build(defaultMap));
//                                    }
//                                });
                                tapNodeSpecification.setDataTypesMap(matchingMap);
                            }
                            DefaultExpressionMatchingMap dataTypesMap = tapNodeSpecification.getDataTypesMap();
                            if (dataTypesMap == null || dataTypesMap.isEmpty()) {
                                try (InputStream dataTypeInputStream = this.getClass().getClassLoader().getResourceAsStream("default-data-types.json")) {
                                    if (dataTypeInputStream != null) {
                                        String dataTypesJson = IOUtils.toString(dataTypeInputStream, StandardCharsets.UTF_8);
                                        if (StringUtils.isNotBlank(dataTypesJson)) {
                                            TapNodeContainer container = InstanceFactory.instance(JsonParser.class).fromJson(dataTypesJson, TapNodeContainer.class);
                                            if (container != null && container.getDataTypes() != null)
                                                tapNodeSpecification.setDataTypesMap(DefaultExpressionMatchingMap.map(container.getDataTypes()));
                                        }
                                    }
                                }
                            }
                            ClassLoader classLoader = clazz.getClassLoader();

                            try {
                                Method method = classLoader.getClass().getMethod("manifest");
                                method.setAccessible(true);
                                tapNodeSpecification.setManifest((Map<String, String>) method.invoke(classLoader));
                            } catch (Throwable throwable) {
                                TapLogger.debug(TAG, "Read manifest failed {}, file path is: {}.", throwable.getMessage(), filePath);
                            }
//                            tapNodeSpecification.setManifest();
                            String connectorType = findConnectorType(clazz);
                            if (connectorType == null) {
                                TapLogger.error(TAG, "Connector class for id {} title {} only have TapConnector annotation, but not implement the necessary methods, file path is: {}, {} will be ignored...", tapNodeSpecification.idAndGroup(), tapNodeSpecification.getName(), filePath, clazz);
                                continue;
                            }
                            TapNodeInfo tapNodeInfo = newerIdGroupTapNodeInfoMap.get(tapNodeSpecification.idAndGroup());
                            if (tapNodeInfo == null) {
                                tapNodeInfo = new TapNodeInfo();
                                tapNodeInfo.setTapNodeSpecification(tapNodeSpecification);
                                tapNodeInfo.setNodeType(connectorType);
                                tapNodeInfo.setNodeClass(clazz);
                                newerIdGroupTapNodeInfoMap.put(tapNodeSpecification.idAndGroup(), tapNodeInfo);
                                TapLogger.debug(TAG, "Found new connector {} type {}, file path is: {}.", tapNodeSpecification.idAndGroup(), connectorType, filePath);
                            } else {
                                TapNodeSpecification specification = tapNodeInfo.getTapNodeSpecification();
                                tapNodeInfo.setTapNodeSpecification(specification);
                                tapNodeInfo.setNodeType(connectorType);
                                tapNodeInfo.setNodeClass(clazz);
                                TapLogger.warn(TAG, "Found newer connector {} type {}, file path is: {}.", tapNodeSpecification.idAndGroup(), connectorType, filePath);
                            }
                        } catch (Throwable throwable) {
                            TapLogger.error(TAG, "Handle tap node specification failed, path {} error {}, file path is: {}.", tapConnectorClass.value(), throwable.getMessage(), filePath);
                        }
                    } else {
                        StringJoiner jarFileName = new StringJoiner("; ");
                        try {
                            Class<java.net.URLClassLoader> urlClassLoader = java.net.URLClassLoader.class;
                            Field ucp = urlClassLoader.getDeclaredField("ucp");
                            ucp.setAccessible(true);
                            Object sunMiscURLClassPath = ucp.get(clazz.getClassLoader());
                            Field loaders = sunMiscURLClassPath.getClass().getDeclaredField("loaders");
                            loaders.setAccessible(true);
                            Object collection = loaders.get(sunMiscURLClassPath);
                            for (Object sunMiscURLClassPathJarLoader : ((Collection<?>) collection).toArray()) {
                                try {
                                    Field loader = sunMiscURLClassPathJarLoader.getClass().getDeclaredField("jar");
                                    loader.setAccessible(true);
                                    Object jarFile = loader.get(sunMiscURLClassPathJarLoader);
                                    JarFile theJarFile = ((JarFile) jarFile);
                                    jarFileName.add(theJarFile.getName());
                                } catch (Throwable t) {
                                    // if we got this far, this is probably not a JAR loader so skip it
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        TapLogger.error(TAG, "Resource {} not found, connector class {} will be ignored, connector jar path is: {}.", tapConnectorClass.value(), clazz, jarFileName.toString());
                    }
                }
            }
            TapLogger.debug(TAG, "--------------TapConnector Classes End-------------");
        }
    }

    private boolean isSource(ConnectorFunctions connectorFunctions){
        return null != connectorFunctions.getBatchReadFunction()
                || null != connectorFunctions.getStreamReadFunction()
                || null != connectorFunctions.getRawDataCallbackFilterFunctionV2()
                || null != connectorFunctions.getRawDataCallbackFilterFunction();
    }

    private boolean isTarget(ConnectorFunctions connectorFunctions){
        return null != connectorFunctions.getWriteRecordFunction();
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
                isSource = isSource(connectorFunctions);
                isTarget = isTarget(connectorFunctions);
            } catch (Throwable e) {
                TapLogger.error(TAG, "Find connector type failed, {} clazz {} will be ignored", e.getMessage(), clazz);
                throw new CoreException(TapAPIErrorCodes.ERROR_FIND_CONNECTOR_TYPE_FAILED, "Find connector class {} type failed, {}", clazz, e.getMessage());
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

    private Set<String> findAllConnectorSuperClassName(Class<?> clazz) {
        Set<String> classNames = new HashSet<>();
        List<Class<?>> classList = ReflectionUtil.getSuperClasses(clazz);
        if (classList != null) {
            for (Class<?> superClass : classList) {
                final Annotation clazzAnnotation = superClass.getAnnotation(TapConnectorClass.class);
                if (clazzAnnotation != null) {
                    classNames.add(superClass.getCanonicalName());
                }
            }
        }
        return classNames;
    }

    @Override
    public Class<? extends Annotation> watchAnnotation() {
        return TapConnectorClass.class;
    }

}
