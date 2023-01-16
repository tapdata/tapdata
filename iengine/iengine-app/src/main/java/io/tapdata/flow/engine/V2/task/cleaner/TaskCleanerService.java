package io.tapdata.flow.engine.V2.task.cleaner;

import io.tapdata.flow.engine.V2.task.OpType;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * @author samuel
 * @Description
 * @create 2022-10-14 10:43
 **/
public class TaskCleanerService {

	public static void clean(TaskCleanerContext taskCleanerContext, String opTypeStr) throws TaskCleanerException {
		checkConstructorParamInvalid(taskCleanerContext, opTypeStr);
		taskCleanerContext.opType(OpType.fromOp(opTypeStr));
		String implementClass = taskCleanerContext.getOpType().getImplementClass();
		Constructor<?> constructor;
		try {
			constructor = Class.forName(implementClass).getConstructor(TaskCleanerContext.class);
		} catch (NoSuchMethodException e) {
			throw new TaskCleanerException(String.format("Task cleaner implement class %s not have constructor(TaskCleanerContext.class)", implementClass), e);
		} catch (ClassNotFoundException e) {
			throw new TaskCleanerException("Cannot found task cleaner implement class: " + implementClass, e);
		}
		Object taskCleaner;
		try {
			taskCleaner = constructor.newInstance(taskCleanerContext);
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
			throw new TaskCleanerException("Init task cleaner error: " + e.getMessage(), e);
		}
		if (taskCleaner instanceof TaskCleaner) {
			((TaskCleaner) taskCleaner).clean();
		} else {
			throw new TaskCleanerException(String.format("Task cleaner %s must extends %s", taskCleaner.getClass().getName(), TaskCleaner.class.getName()));
		}
	}

	private static void checkConstructorParamInvalid(TaskCleanerContext taskCleanerContext, String opTypeStr) throws TaskCleanerException {
		if (StringUtils.isBlank(opTypeStr)) {
			throw new TaskCleanerException(new IllegalArgumentException("Op type cannot be empty"));
		}
		OpType opType = OpType.fromOp(opTypeStr);
		if (null == opType) {
			throw new TaskCleanerException(new IllegalArgumentException("Op type is invalid: " + opTypeStr));
		}
		if (StringUtils.isBlank(taskCleanerContext.getTaskId())) {
			throw new TaskCleanerException(new IllegalArgumentException("Task id cannot be empty"));
		}
		if (null == taskCleanerContext.getClientMongoOperator()) {
			throw new TaskCleanerException(new IllegalArgumentException("Client operator cannot be null"));
		}
	}
}
