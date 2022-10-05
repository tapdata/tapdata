package io.tapdata.wsserver.channels.gateway;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.ClassAnnotationHandler;
import io.tapdata.entity.utils.ReflectionUtil;
import io.tapdata.wsserver.channels.annotation.GatewaySession;
import org.apache.commons.lang3.StringUtils;

import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Bean
public class GatewaySessionAnnotationHandler extends ClassAnnotationHandler {
	private static final String TAG = GatewaySessionAnnotationHandler.class.getSimpleName();

	private final Map<String, Class<? extends GatewaySessionHandler>> idTypeClassMap = new ConcurrentHashMap<>();

	@Override
	public void handle(Set<Class<?>> classes) throws CoreException {
        if (classes != null) {
            for (Class<?> clazz : classes) {
                GatewaySession annotation = clazz.getAnnotation(GatewaySession.class);
                if (annotation != null) {
					String idType = annotation.idType();
					if(StringUtils.isEmpty(idType)) {
						TapLogger.warn(TAG, "GatewaySession annotation is found on class {}, but not idType specified which is a must. Ignore this class... idType {}", clazz, idType);
						continue;
					}
                    if (!GatewaySessionHandler.class.isAssignableFrom(clazz)) {
                        TapLogger.warn(TAG, "GatewaySession annotation is found on class {}, but not implemented GatewaySessionHandler which is a must. Ignore this class... idType {}", clazz, idType);
                        continue;
                    }
                    if (!ReflectionUtil.canBeInitiated(clazz)) {
                        TapLogger.warn(TAG, "GatewaySession annotation is found on class {}, but not be initialized with empty parameter which is a must. Ignore this class... idType {}", clazz, idType);
                        continue;
                    }
					//noinspection unchecked
					Class<?> oldClazz = idTypeClassMap.putIfAbsent(idType, (Class<? extends GatewaySessionHandler>) clazz);
					if(oldClazz != null) {
						TapLogger.warn(TAG, "GatewaySession annotation is found on class {}, but old class {} already exists. Ignore this class... idType {}", clazz, oldClazz, idType);
					}
                }
            }
        }
	}

	@Override
	public Class<? extends Annotation> watchAnnotation() {
		return GatewaySession.class;
	}

	public Class<? extends GatewaySessionHandler> getGatewaySessionHandlerClass(String idType) {
		return idTypeClassMap.get(idType);
	}

	public boolean isEmpty() {
		return idTypeClassMap.isEmpty();
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(GatewaySessionAnnotationHandler.class.getSimpleName()).append(": ");
		for(Map.Entry<String, Class<? extends GatewaySessionHandler>> entry : idTypeClassMap.entrySet()) {
			builder.append("\t").append(entry.getKey()).append("->").append(entry.getValue()).append("; \n");
		}
		return builder.toString();
	}
}
