package io.tapdata.aspect.task.impl;

import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.ClassAnnotationHandler;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class AspectTaskSessionAnnotationHandler extends ClassAnnotationHandler {
	private static final String TAG = AspectTaskSessionAnnotationHandler.class.getSimpleName();
	private Map<String, Collection<TaskSessionClassHolder>> aspectTaskSessionMap = new ConcurrentHashMap<>();
	private Map<String, Collection<TaskSessionClassHolder>> newAspectTaskSessionMap;

	@Override
	public void handle(Set<Class<?>> classes) throws CoreException {
		if (classes != null) {
			newAspectTaskSessionMap = new ConcurrentHashMap<>();
			TapLogger.debug(TAG, "--------------AspectTask Classes Start-------------");
			for (Class<?> clazz : classes) {
				AspectTaskSession aspectTaskSession = clazz.getAnnotation(AspectTaskSession.class);
				if (aspectTaskSession != null) {
					if (!AspectTask.class.isAssignableFrom(clazz)) {
						TapLogger.error(TAG, "AspectTask {} don't implement {}, will be ignored", clazz, AspectTask.class);
						continue;
					}
					Class<? extends AspectTask> observerClass = (Class<? extends AspectTask>) clazz;
					String aspectClass = aspectTaskSession.value();
					int order = aspectTaskSession.order();
					String[] includeTypes = aspectTaskSession.includeTypes();
					boolean ignoreErrors = aspectTaskSession.ignoreErrors();
					List<String> includeList = new ArrayList<>();
					if(includeTypes != null) {
						for(String include : includeTypes) {
							if(!include.trim().equals("") && !includeList.contains(include)) {
								includeList.add(include);
							}
						}
					}
					String[] excludeTypes = aspectTaskSession.excludeTypes();
					List<String> excludeList = new ArrayList<>();
					if(excludeTypes != null) {
						for(String exclude : excludeTypes) {
							if(!exclude.trim().equals("") && !excludeList.contains(exclude)) {
								excludeList.add(exclude);
							}
						}
					}

					//Check class can be initialized for non-args constructor
					String canNotInitialized = null;
					try {
						Constructor<?> constructor = observerClass.getConstructor();
						if (!Modifier.isPublic(constructor.getModifiers())) {
							canNotInitialized = "Constructor is not public";
						}
					} catch (Throwable e) {
						canNotInitialized = e.getMessage();
					}
					if (canNotInitialized != null) {
						TapLogger.error(TAG, "AspectTask {} don't have non-args public constructor, will be ignored, message {}", clazz, canNotInitialized);
						continue;
					}

					Collection<TaskSessionClassHolder> implClasses = newAspectTaskSessionMap.get(aspectClass);
					if (implClasses == null) {
						implClasses = Collections.synchronizedSortedSet(new TreeSet<>());
						implClasses.add(new TaskSessionClassHolder().taskClass(observerClass).excludeTypes(excludeList).includeTypes(includeList).order(order).ignoreErrors(ignoreErrors));
						newAspectTaskSessionMap.put(aspectClass, implClasses);
						TapLogger.debug(TAG, "(New array) AspectTask {} for Aspect {} will be applied", observerClass, aspectClass);
					} else {
						implClasses.add(new TaskSessionClassHolder().taskClass(observerClass).excludeTypes(excludeList).includeTypes(includeList).order(order).ignoreErrors(ignoreErrors));
						TapLogger.debug(TAG, "(Exist array) AspectTask {} for Aspect {} will be applied", clazz, aspectClass);
					}
				}
			}
			TapLogger.debug(TAG, "--------------AspectTask Classes End-------------");
		}
		apply();
	}

	public void apply() {
		if (newAspectTaskSessionMap != null) {
			aspectTaskSessionMap = newAspectTaskSessionMap;
			newAspectTaskSessionMap = null;
		}
	}

	public Map<String, Collection<TaskSessionClassHolder>> getAspectTaskSessionMap() {
		return aspectTaskSessionMap;
	}

	@Override
	public Class<? extends Annotation> watchAnnotation() {
		return AspectTaskSession.class;
	}
}
