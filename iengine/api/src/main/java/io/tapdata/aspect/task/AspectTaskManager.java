package io.tapdata.aspect.task;

import java.util.List;

/**
 *
 * Use Annotation AspectTaskSession and abstract class AspectTask to describe
 */
public interface AspectTaskManager {

	List<AspectTask> getAspectTasks(String taskId);
}
