package io.tapdata.flow.engine.V2.task.preview.tasklet;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.task.preview.PreviewReadOperationQueue;
import io.tapdata.flow.engine.V2.task.preview.TaskPreviewException;

/**
 * @author samuel
 * @Description
 * @create 2024-09-25 11:38
 **/
public interface PreviewReadTasklet {
	void execute(TaskDto taskDto, PreviewReadOperationQueue previewReadOperationQueue) throws TaskPreviewException;
}
