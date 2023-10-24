package io.tapdata.async.master;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.ClassAnnotationHandler;
import io.tapdata.modules.api.async.master.AsyncJobClass;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class AsyncJobAnnotationHandler extends ClassAnnotationHandler {
	private static final String TAG = AsyncJobAnnotationHandler.class.getSimpleName();
	private Map<String, Class<? extends Job>> asyncJobMap = new ConcurrentHashMap<>();
	private Map<String, Class<? extends Job>> newAsyncJobMap;

	@Override
	public void handle(Set<Class<?>> classes) throws CoreException {
		if (classes != null) {
			newAsyncJobMap = new ConcurrentHashMap<>();
			TapLogger.debug(TAG, "--------------AsyncJob Classes Start-------------");
			for (Class<?> clazz : classes) {
				AsyncJobClass asyncJobClass = clazz.getAnnotation(AsyncJobClass.class);
				if (asyncJobClass != null) {
					if (!Job.class.isAssignableFrom(clazz)) {
						TapLogger.error(TAG, "AsyncJob {} don't implement {}, will be ignored", clazz, Job.class);
						continue;
					}
					//noinspection unchecked
					Class<? extends Job> jobClass = (Class<? extends Job>) clazz;
					String jobType = asyncJobClass.value();

					//Check class can be initialized for non-args constructor
					String canNotInitialized = null;
					try {
						Constructor<?> constructor = jobClass.getConstructor();
						if (!Modifier.isPublic(constructor.getModifiers())) {
							canNotInitialized = "Constructor is not public";
						}
					} catch (Throwable e) {
						canNotInitialized = e.getMessage();
					}
					if (canNotInitialized != null) {
						TapLogger.error(TAG, "AsyncJob {} don't have non-args public constructor, will be ignored, message {}", clazz, canNotInitialized);
						continue;
					}

					Class<? extends Job> current = newAsyncJobMap.putIfAbsent(jobType, jobClass);
					if(current != null) {
						TapLogger.warn(TAG, "AsyncJob type {} has jobClass {} already, new jobClass {} will be ignored", jobType, current, jobClass);
						continue;
					} else {
						TapLogger.debug(TAG, "AsyncJob jobClass {} for type {} will be applied", jobType, jobClass);
					}
				}
			}
			TapLogger.debug(TAG, "--------------AsyncJob Classes End-------------");
		}
		apply();
	}

	public void apply() {
		if (newAsyncJobMap != null) {
			asyncJobMap = newAsyncJobMap;
			newAsyncJobMap = null;
		}
	}

	public Map<String, Class<? extends Job>> getAsyncJobMap() {
		return asyncJobMap;
	}

	@Override
	public Class<? extends Annotation> watchAnnotation() {
		return AsyncJobClass.class;
	}
}
