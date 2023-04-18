package io.tapdata.pdk.core.tapnode;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.apis.TapProcessor;
import io.tapdata.pdk.apis.annotations.TapProcessorClass;
import io.tapdata.entity.error.CoreException;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TapProcessorAnnotationHandler extends TapBaseAnnotationHandler {
    private static final String TAG = TapProcessorAnnotationHandler.class.getSimpleName();
    public TapProcessorAnnotationHandler() {
        super();
    }

    @Override
    public void handle(Set<Class<?>> classes) throws CoreException {
        if(classes != null && !classes.isEmpty()) {
            newerIdGroupTapNodeInfoMap = new ConcurrentHashMap<>();
            TapLogger.debug(TAG, "--------------TapProcessor Classes Start------------- {}", classes.size());
            for(Class<?> clazz : classes) {
                TapProcessorClass tapProcessorClass = clazz.getAnnotation(TapProcessorClass.class);
                if(tapProcessorClass != null) {
                    URL url = clazz.getClassLoader().getResource(tapProcessorClass.value());
                    if(url != null) {
                        TapNodeSpecification tapNodeSpecification = null;
                        try {
                            InputStream is = url.openStream();
                            String json = IOUtils.toString(is, StandardCharsets.UTF_8);
                            TapNodeContainer tapNodeContainer = InstanceFactory.instance(JsonParser.class).fromJson(json, TapNodeContainer.class);
                            tapNodeSpecification = tapNodeContainer.getProperties();
                            String errorMessage = null;
                            if(tapNodeSpecification == null)
                                errorMessage = "Specification not found";
                            if(errorMessage == null)
                                errorMessage = tapNodeSpecification.verify();
                            if(errorMessage != null) {
                                TapLogger.warn(TAG, "Tap node specification is illegal, will be ignored, path {} content {} errorMessage {}", tapProcessorClass.value(), json, errorMessage);
                                continue;
                            }
                            String connectorType = findConnectorType(clazz);
                            if(connectorType == null) {
                                TapLogger.error(TAG, "Processor class for id {} title {} only have TapProcessor annotation, but not implement TapProcessor which is must, {} will be ignored...", tapNodeSpecification.getId(), tapNodeSpecification.getName(), clazz);
                                continue;
                            }
                            TapNodeInfo tapNodeInfo = newerIdGroupTapNodeInfoMap.get(tapNodeSpecification.idAndGroup());
                            if(tapNodeInfo == null) {
                                tapNodeInfo = new TapNodeInfo();
                                tapNodeInfo.setTapNodeSpecification(tapNodeSpecification);
                                tapNodeInfo.setNodeType(connectorType);
                                tapNodeInfo.setNodeClass(clazz);
                                newerIdGroupTapNodeInfoMap.put(tapNodeSpecification.idAndGroup(), tapNodeInfo);
                                TapLogger.debug(TAG, "Found new processor {} type {} version {} buildNumber {}", tapNodeSpecification.idAndGroup(), connectorType, tapNodeSpecification.getVersion(), tapNodeSpecification.getVersion());
                            } else {
                                TapNodeSpecification specification = tapNodeInfo.getTapNodeSpecification();
                                tapNodeInfo.setTapNodeSpecification(specification);
                                tapNodeInfo.setNodeType(connectorType);
                                tapNodeInfo.setNodeClass(clazz);
                                TapLogger.warn(TAG, "Found newer processor {} type {}", tapNodeSpecification.idAndGroup(), connectorType);
                            }
                        } catch(Throwable throwable) {
                            TapLogger.error(TAG, "Handle tap node specification failed, path {} error {}", tapProcessorClass.value(), throwable.getMessage());
                        }
                    } else {
                        TapLogger.error(TAG, "Resource {} not found, processor class {} will be ignored", tapProcessorClass.value(), clazz);
                    }
                }
            }
            TapLogger.debug(TAG, "--------------TapProcessor Classes End-------------");
        }
    }

    private String findConnectorType(Class<?> clazz) {
        boolean isProcessor = false;
        if(TapProcessor.class.isAssignableFrom(clazz)) {
            isProcessor = true;
        }
        if(isProcessor) {
            return TapNodeInfo.NODE_TYPE_PROCESSOR;
        }
        return null;
    }

    @Override
    public Class<? extends Annotation> watchAnnotation() {
        return TapProcessorClass.class;
    }

}
