package io.tapdata.aspect.task.impl;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.AspectTaskManager;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.aspect.AspectManager;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.ClassAnnotationManager;
import io.tapdata.entity.utils.ClassFactory;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.*;

@Implementation(AspectTaskManager.class)
public class AspectTaskManagerImpl implements AspectTaskManager {
	private static final String TAG = AspectTaskManagerImpl.class.getSimpleName();
	private final AspectTaskSessionAnnotationHandler aspectTaskSessionAnnotationHandler;
	private Map<String, Collection<TaskSessionClassHolder>> taskSessionMap;

	public AspectTaskManagerImpl() {
		aspectTaskSessionAnnotationHandler = new AspectTaskSessionAnnotationHandler();

		ClassAnnotationManager classAnnotationManager = ClassFactory.create(ClassAnnotationManager.class);
		if (classAnnotationManager != null) {
			classAnnotationManager
					.registerClassAnnotationHandler(aspectTaskSessionAnnotationHandler);
			String scanPackage = CommonUtils.getProperty("pdk_aspect_scan_package", "io.tapdata,com.tapdata");
			String[] packages = scanPackage.split(",");
			classAnnotationManager.scan(packages, this.getClass().getClassLoader());

			taskSessionMap = aspectTaskSessionAnnotationHandler.getAspectTaskSessionMap();
			if (!taskSessionMap.isEmpty()) {
				AspectManager aspectManager = InstanceFactory.instance(AspectManager.class);
				if (aspectManager != null) {
					final int order = 10000;
					aspectManager.registerAspectObserver(TaskStartAspect.class, order, aspect -> {
						TaskDto task = aspect.getTask();
						if (task == null || task.getId() == null) {
							TapLogger.warn(TAG, "SubTaskDto is missing or taskId is null for TaskStartAspect, task {}", task);
							return;
						}
						Collection<TaskSessionClassHolder> classHolders = taskSessionMap.get("default");
						if (classHolders != null) {
							for (TaskSessionClassHolder classHolder : classHolders) {
								if(classHolder.isTaskSupported(task))
									classHolder.ensureTaskSessionCreated(aspect);
							}
						}
					}, false);
					aspectManager.registerAspectObserver(TaskStopAspect.class, order, aspect -> {
						TaskDto task = aspect.getTask();
						if (task == null || task.getId() == null) {
							TapLogger.warn(TAG, "TaskDto is missing or taskId is null for TaskStopAspect, task {}", task);
							return;
						}
						Collection<TaskSessionClassHolder> classHolders = taskSessionMap.get("default");
						if (classHolders != null) {
							RuntimeException runtimeException = null;
							for (TaskSessionClassHolder classHolder : classHolders) {
								try {
									classHolder.ensureTaskSessionStopped(aspect);
								} catch (Throwable throwable) {
									if(!classHolder.isIgnoreErrors()) {
										if(runtimeException == null)
											runtimeException = new RuntimeException(throwable);
										else
											runtimeException.addSuppressed(throwable);
									}
								}
							}
							if(runtimeException != null) {
								throw runtimeException;
							}
						}
					}, false);
				}
			}
		}
	}

	@Override
	public List<AspectTask> getAspectTasks(String taskId) {
		Collection<TaskSessionClassHolder> classHolders = taskSessionMap.get("default");
		if(classHolders != null) {
			List<AspectTask> aspectTasks = new ArrayList<>();
			for (TaskSessionClassHolder classHolder : classHolders) {
				AspectTask aspectTask = classHolder.getAspectTasks(taskId);
				if(aspectTask != null)
					aspectTasks.add(aspectTask);
			}
			return aspectTasks;
		}
		return Collections.emptyList();
	}

}
