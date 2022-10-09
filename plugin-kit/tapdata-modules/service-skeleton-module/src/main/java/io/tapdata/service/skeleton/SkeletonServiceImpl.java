package io.tapdata.service.skeleton;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.ReflectionUtil;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.service.SkeletonService;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

@Implementation(SkeletonService.class)
public class SkeletonServiceImpl implements SkeletonService {
	@Bean
	private ServiceSkeletonAnnotationHandler serviceSkeletonAnnotationHandler;
	private final static String SERVICE_ENGINE = "tapdata_engine";

	@Override
	public CompletableFuture<Object> call(String className, String method, Object... args) {
		return call(className, method, Object.class, args);
	}

	@Override
	public <T> CompletableFuture<T> call(String className, String method, Class<T> responseClass, Object... args) {
		CompletableFuture<T> future = new CompletableFuture<>();
		long crc = ReflectionUtil.getCrc(className, method, SERVICE_ENGINE);
		ServiceSkeletonAnnotationHandler.SkeletonMethodMapping methodMapping = serviceSkeletonAnnotationHandler.getMethodMapping(crc);
		if(methodMapping == null) {
			future.completeExceptionally(new CoreException(NetErrors.ERROR_METHODREQUEST_METHODNOTFOUND, "Method not found for class {} method {}", className, method));
		} else {
			MethodRequest methodRequest = MethodRequest.create().crc(crc).args(args).specifiedReturnClass(responseClass);
			CommonUtils.handleAnyError(() -> {
				MethodResponse methodResponse = methodMapping.invoke(methodRequest);
				CoreException coreException = methodResponse.getException();
				if(coreException != null) {
					future.completeExceptionally(coreException);
				} else {
					//noinspection unchecked
					future.complete((T) methodResponse.getReturnObject());
				}
			}, future::completeExceptionally);
		}
		return future;
	}
}
