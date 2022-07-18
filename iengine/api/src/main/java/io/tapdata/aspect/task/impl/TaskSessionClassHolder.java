package io.tapdata.aspect.task.impl;

import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.aspect.DataNodeAspect;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.entity.aspect.*;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class TaskSessionClassHolder  implements Comparable<TaskSessionClassHolder> {
	private static final String TAG = TaskSessionClassHolder.class.getSimpleName();
	private final Map<String, AspectTaskEx> aspectTaskMap = new ConcurrentHashMap<>();
	private Class<? extends AspectTask> taskClass;
	private AspectObserver<Aspect> aspectObserver;
	private AspectInterceptor<Aspect> aspectInterceptor;
	public TaskSessionClassHolder() {
		aspectObserver = this::observeNodeAspect;
		aspectInterceptor = this::interceptNodeAspect;
	}
	public TaskSessionClassHolder taskClass(Class<? extends AspectTask> taskClass) {
		this.taskClass = taskClass;
		return this;
	}
	private int order;
	public TaskSessionClassHolder order(int order) {
		this.order = order;
		return this;
	}

	public Class<? extends AspectTask> getTaskClass() {
		return taskClass;
	}

	public void setTaskClass(Class<? extends AspectTask> taskClass) {
		this.taskClass = taskClass;
	}

	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}

	@Override
	public int compareTo(TaskSessionClassHolder sessionClassHolder) {
//        if(order == interceptorClassHolder.order)
//            return 0;
		return order > sessionClassHolder.order ? 1 : -1;
	}
	private <T extends Aspect> void observeNodeAspect(T aspect) {
		String theTaskId = null;
		if(aspect instanceof DataNodeAspect) {
			DataProcessorContext dataProcessorContext = ((DataNodeAspect<?>) aspect).getDataProcessorContext();
			if(dataProcessorContext != null) {
				SubTaskDto taskDto = dataProcessorContext.getSubTaskDto();
				if(taskDto != null && taskDto.getId() != null) {
					theTaskId = taskDto.getId().toString();
					AspectTaskEx aspectTask = aspectTaskMap.get(theTaskId);
					if(aspectTask != null) {
						aspectTask.aspectTask.onObserveAspect(aspect);
						return;
					}
				}
			}
		}
		TapLogger.warn(TAG, "Observe aspect {} is illegal for taskId {}, no aspect task will handle it. ", aspect, theTaskId);
	}

	private <T extends Aspect> AspectInterceptResult interceptNodeAspect(T aspect) {
		String theTaskId = null;
		if(aspect instanceof DataNodeAspect) {
			DataProcessorContext dataProcessorContext = ((DataNodeAspect<?>) aspect).getDataProcessorContext();
			if(dataProcessorContext != null) {
				SubTaskDto taskDto = dataProcessorContext.getSubTaskDto();
				if(taskDto != null && taskDto.getId() != null) {
					theTaskId = taskDto.getId().toString();
					AspectTaskEx aspectTask = aspectTaskMap.get(theTaskId);
					if(aspectTask != null) {
						return aspectTask.aspectTask.onInterceptAspect(aspect);
					}
				}
			}
		}
		TapLogger.warn(TAG, "Intercept aspect {} is illegal for taskId {}, no aspect task will handle it. ", aspect, theTaskId);
		return null;
	}
	public void ensureTaskSessionCreated(SubTaskDto task) {
		String taskId = task.getId().toString();
		AtomicReference<AspectTaskEx> newRef = new AtomicReference<>();
		AspectTaskEx theAspectTask = aspectTaskMap.computeIfAbsent(taskId, id -> {
			AspectTaskEx aspectTask = newAspectTask(task);
			newRef.set(aspectTask);
			return aspectTask;
		});
		if(newRef.get() != null && theAspectTask.equals(newRef.get())) {
			//new created AspectTask
			final int order = 10000;
			List<Class<? extends Aspect>> observerClasses = theAspectTask.aspectTask.observeAspects();
			AspectManager aspectManager = InstanceFactory.instance(AspectManager.class);
			if(observerClasses != null && !observerClasses.isEmpty()) {
				for(Class<? extends Aspect> aspectClass : observerClasses) {
					if(DataNodeAspect.class.isAssignableFrom(aspectClass)) {
						aspectManager.registerObserver(aspectClass, order, aspectObserver);
					} else {
						aspectManager.registerObserver(aspectClass, order, theAspectTask.aspectObserver);
					}
				}
			}
			List<Class<? extends Aspect>> interceptClasses = theAspectTask.aspectTask.interceptAspects();
			if(interceptClasses != null && !interceptClasses.isEmpty()) {
				for(Class<? extends Aspect> aspectClass : interceptClasses) {
					if(DataNodeAspect.class.isAssignableFrom(aspectClass)) {
						aspectManager.registerInterceptor(aspectClass, order, aspectInterceptor);
					} else {
						aspectManager.registerInterceptor(aspectClass, order, theAspectTask.aspectInterceptor);
					}
				}
			}
			CommonUtils.ignoreAnyError(theAspectTask.aspectTask::onStart, TAG);
		}
	}

	private AspectTaskEx newAspectTask(SubTaskDto task) {
		try {
			AspectTask aspectTask = taskClass.getConstructor().newInstance();
			aspectTask.setTask(task);
			return new AspectTaskEx(aspectTask);
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}

	public void ensureTaskSessionStopped(SubTaskDto task) {
		String taskId = task.getId().toString();
		AspectTaskEx aspectTask = aspectTaskMap.remove(taskId);
		if(aspectTask != null) {
			CommonUtils.ignoreAnyError(aspectTask.aspectTask::onStop, TAG);

			List<Class<? extends Aspect>> observerClasses = aspectTask.aspectTask.observeAspects();
			AspectManager aspectManager = InstanceFactory.instance(AspectManager.class);
			if(observerClasses != null && !observerClasses.isEmpty()) {
				for(Class<? extends Aspect> aspectClass : observerClasses) {
					if(DataNodeAspect.class.isAssignableFrom(aspectClass)) {
						if(aspectTaskMap.isEmpty())
							aspectManager.unregisterObserver(aspectClass, aspectObserver);
					} else {
						aspectManager.unregisterObserver(aspectClass, aspectTask.aspectObserver);
					}
				}
			}

			List<Class<? extends Aspect>> interceptClasses = aspectTask.aspectTask.interceptAspects();
			if(interceptClasses != null && !interceptClasses.isEmpty()) {
				for(Class<? extends Aspect> aspectClass : interceptClasses) {
					if(DataNodeAspect.class.isAssignableFrom(aspectClass)) {
						if(aspectTaskMap.isEmpty())
							aspectManager.unregisterInterceptor(aspectClass, aspectInterceptor);
					} else {
						aspectManager.unregisterInterceptor(aspectClass, aspectTask.aspectInterceptor);
					}
				}
			}
		}
	}

	class AspectTaskEx {
		AspectTask aspectTask;
		AspectObserver<Aspect> aspectObserver;
		AspectInterceptor<Aspect> aspectInterceptor;
		AspectTaskEx(AspectTask aspectTask) {
			this.aspectTask = aspectTask;
			aspectObserver = aspect -> {
				if(aspectTask != null) {
					aspectTask.onObserveAspect(aspect);
				}
			};
			aspectInterceptor = aspect -> {
				if(aspectTask != null) {
					return aspectTask.onInterceptAspect(aspect);
				}
				return null;
			};
		}

	}

}
