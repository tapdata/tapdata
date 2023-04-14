package io.tapdata.aspect.task.impl;

import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.DataNodeAspect;
import io.tapdata.aspect.ProcessorNodeAspect;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.entity.aspect.*;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class TaskSessionClassHolder implements Comparable<TaskSessionClassHolder> {
	private static final String TAG = TaskSessionClassHolder.class.getSimpleName();
	private final Map<String, AspectTaskEx> aspectTaskMap = new ConcurrentHashMap<>();
	private Class<? extends AspectTask> taskClass;
	private final AspectObserver<Aspect> aspectObserver;
	private final AspectInterceptor<Aspect> aspectInterceptor;

	private boolean ignoreErrors = true;
	public TaskSessionClassHolder ignoreErrors(boolean ignoreErrors) {
		this.ignoreErrors = ignoreErrors;
		return this;
	}

	public TaskSessionClassHolder() {
		aspectObserver = this::observeNodeAspect;
		aspectInterceptor = this::interceptNodeAspect;
	}

	public TaskSessionClassHolder taskClass(Class<? extends AspectTask> taskClass) {
		this.taskClass = taskClass;
		return this;
	}

	private List<String> includeTypes;
	public TaskSessionClassHolder includeTypes(List<String> includeTypes) {
		this.includeTypes = includeTypes;
		return this;
	}

	private List<String> excludeTypes;
	public TaskSessionClassHolder excludeTypes(List<String> excludeTypes) {
		this.excludeTypes = excludeTypes;
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
		TaskDto taskDto = getSubTaskDto(aspect);
		if (taskDto != null && taskDto.getId() != null) {
			theTaskId = taskDto.getId().toString();
			AspectTaskEx aspectTask = aspectTaskMap.get(theTaskId);
			if (aspectTask != null) {
				aspectTask.aspectTask.onObserveAspect(aspect);
				return;
			}
		}
//		TapLogger.debug(TAG, "Observe aspect {} is illegal for taskId {}, no aspect task will handle it. ", aspect, theTaskId);
	}

	@Nullable
	private <T extends Aspect> TaskDto getSubTaskDto(T aspect) {
		TaskDto taskDto = null;
		if (aspect instanceof ProcessorNodeAspect) {
			ProcessorBaseContext processorBaseContext = ((ProcessorNodeAspect<?>) aspect).getProcessorBaseContext();
			if (processorBaseContext != null) {
				taskDto = processorBaseContext.getTaskDto();
			}
		}

		if (aspect instanceof DataNodeAspect) {
			DataProcessorContext dataProcessorContext = ((DataNodeAspect<?>) aspect).getDataProcessorContext();
			if (dataProcessorContext != null) {
				taskDto = dataProcessorContext.getTaskDto();
			}
		}
		return taskDto;
	}

	private <T extends Aspect> AspectInterceptResult interceptNodeAspect(T aspect) {
		String theTaskId = null;
		TaskDto taskDto = getSubTaskDto(aspect);
		if (taskDto != null && taskDto.getId() != null) {
			theTaskId = taskDto.getId().toString();
			AspectTaskEx aspectTask = aspectTaskMap.get(theTaskId);
			if (aspectTask != null) {
				return aspectTask.aspectTask.onInterceptAspect(aspect);
			}
		}
//		TapLogger.debug(TAG, "Intercept aspect {} is illegal for taskId {}, no aspect task will handle it. ", aspect, theTaskId);
		return null;
	}

	public synchronized void ensureTaskSessionCreated(TaskStartAspect startAspect) {
		TaskDto task = startAspect.getTask();
		Log log = startAspect.getLog();

		String taskId = task.getId().toString();
		AtomicReference<AspectTaskEx> newRef = new AtomicReference<>();
		AspectTaskEx theAspectTask = aspectTaskMap.computeIfAbsent(taskId, id -> {
			AspectTaskEx aspectTask = newAspectTask(task, log);
			newRef.set(aspectTask);
			return aspectTask;
		});
		if (newRef.get() == null || !theAspectTask.equals(newRef.get())) {
			ensureTaskSessionStopped(new TaskStopAspect().task(theAspectTask.aspectTask.getTask())
					.error(new RuntimeException("Force call stop aspect")));

			aspectTaskMap.computeIfAbsent(taskId, id -> {
				AspectTaskEx aspectTask = newAspectTask(task, log);
				newRef.set(aspectTask);
				return aspectTask;
			});
		}



		//new created AspectTask
//			final int order = 10000;
		AspectTaskEx aspectTaskEx = newRef.get();
		List<Class<? extends Aspect>> observerClasses = aspectTaskEx.aspectTask.observeAspects();
		AspectManager aspectManager = InstanceFactory.instance(AspectManager.class);
		if (observerClasses != null && !observerClasses.isEmpty()) {
			for (Class<? extends Aspect> aspectClass : observerClasses) {
				if (DataNodeAspect.class.isAssignableFrom(aspectClass) || ProcessorNodeAspect.class.isAssignableFrom(aspectClass)) {
					aspectManager.registerObserver(aspectClass, order, aspectObserver, ignoreErrors);
				} else {
					aspectManager.registerObserver(aspectClass, order, aspectTaskEx.aspectObserver, ignoreErrors);
				}
			}
		}
		List<Class<? extends Aspect>> interceptClasses = aspectTaskEx.aspectTask.interceptAspects();
		if (interceptClasses != null && !interceptClasses.isEmpty()) {
			for (Class<? extends Aspect> aspectClass : interceptClasses) {
				if (DataNodeAspect.class.isAssignableFrom(aspectClass) || ProcessorNodeAspect.class.isAssignableFrom(aspectClass)) {
					aspectManager.registerInterceptor(aspectClass, order, aspectInterceptor, ignoreErrors);
				} else {
					aspectManager.registerInterceptor(aspectClass, order, aspectTaskEx.aspectInterceptor, ignoreErrors);
				}
			}
		}
		if(ignoreErrors) {
			CommonUtils.ignoreAnyError(() -> aspectTaskEx.aspectTask.onStart(startAspect), TAG);
		} else {
			CommonUtils.handleAnyError(() -> aspectTaskEx.aspectTask.onStart(startAspect));
		}
	}

	private AspectTaskEx newAspectTask(TaskDto task, Log log) {
		try {
			AspectTask aspectTask = taskClass.getConstructor().newInstance();
			InstanceFactory.injectBean(aspectTask);
			aspectTask.setTask(task);
			aspectTask.setLog(log);
			return new AspectTaskEx(aspectTask);
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException |
				 NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}

	public void ensureTaskSessionStopped(TaskStopAspect stopAspect) {
		String taskId = stopAspect.getTask().getId().toString();
		ensureTaskSessionStopped(taskId, stopAspect);
	}
	public synchronized void ensureTaskSessionStopped(String taskId, TaskStopAspect stopAspect) {
		if(taskId == null)
			return;
		AspectTaskEx aspectTask = aspectTaskMap.remove(taskId);
		if (aspectTask != null) {
			if(ignoreErrors)
				CommonUtils.ignoreAnyError(() -> aspectTask.aspectTask.onStop(stopAspect), TAG);
			else
				CommonUtils.handleAnyError(() -> aspectTask.aspectTask.onStop(stopAspect));

			List<Class<? extends Aspect>> observerClasses = aspectTask.aspectTask.observeAspects();
			AspectManager aspectManager = InstanceFactory.instance(AspectManager.class);
			if (observerClasses != null && !observerClasses.isEmpty()) {
				for (Class<? extends Aspect> aspectClass : observerClasses) {
					if (DataNodeAspect.class.isAssignableFrom(aspectClass)) {
						if (aspectTaskMap.isEmpty())
							aspectManager.unregisterObserver(aspectClass, aspectObserver);
					} else {
						aspectManager.unregisterObserver(aspectClass, aspectTask.aspectObserver);
					}
				}
			}

			List<Class<? extends Aspect>> interceptClasses = aspectTask.aspectTask.interceptAspects();
			if (interceptClasses != null && !interceptClasses.isEmpty()) {
				for (Class<? extends Aspect> aspectClass : interceptClasses) {
					if (DataNodeAspect.class.isAssignableFrom(aspectClass)) {
						if (aspectTaskMap.isEmpty())
							aspectManager.unregisterInterceptor(aspectClass, aspectInterceptor);
					} else {
						aspectManager.unregisterInterceptor(aspectClass, aspectTask.aspectInterceptor);
					}
				}
			}
		}
	}

	public boolean isTaskSupported(TaskDto task) {
		if(task != null && task.getSyncType() != null) {
			String syncType = task.getSyncType();
			if(excludeTypes != null && excludeTypes.contains(syncType)) {
				return false;
			}
			if(includeTypes != null && !includeTypes.isEmpty() && !includeTypes.contains(syncType)) {
				return false;
			}
			return true;
		}
		return false;
	}

	static class AspectTaskEx {
		AspectTask aspectTask;
		AspectObserver<Aspect> aspectObserver;
		AspectInterceptor<Aspect> aspectInterceptor;

		AspectTaskEx(AspectTask aspectTask) {
			this.aspectTask = aspectTask;
			aspectObserver = aspect -> {
				if (aspectTask != null) {
					aspectTask.onObserveAspect(aspect);
				}
			};
			aspectInterceptor = aspect -> {
				if (aspectTask != null) {
					return aspectTask.onInterceptAspect(aspect);
				}
				return null;
			};
		}
	}

	public AspectTask getAspectTasks(String taskId) {
		AspectTaskEx aspectTaskEx = aspectTaskMap.get(taskId);
		if(aspectTaskEx != null) {
			return aspectTaskEx.aspectTask;
		}
		return null;
	}

	public void setIgnoreErrors(boolean ignoreErrors) {
		this.ignoreErrors = ignoreErrors;
	}

	public boolean isIgnoreErrors() {
		return ignoreErrors;
	}
}
