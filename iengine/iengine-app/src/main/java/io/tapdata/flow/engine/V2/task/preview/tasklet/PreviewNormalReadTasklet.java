package io.tapdata.flow.engine.V2.task.preview.tasklet;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.task.preview.PreviewReadOperationQueue;
import io.tapdata.flow.engine.V2.task.preview.TaskPreviewException;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewFinishReadOperation;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewReadOperation;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2024-09-25 11:39
 **/
public class PreviewNormalReadTasklet implements PreviewReadTasklet {
	@Override
	public void execute(TaskDto taskDto, PreviewReadOperationQueue previewReadOperationQueue) throws TaskPreviewException {
		DAG dag = taskDto.getDag();
		List<Node> sourceNodes = dag.getSourceNodes();
		for (Node sourceNode : sourceNodes) {
			PreviewReadOperation previewReadOperation = new PreviewReadOperation(sourceNode.getId(), taskDto.getPreviewRows());
			TapAdvanceFilter filter = TapAdvanceFilter.create().limit(taskDto.getPreviewRows());
			previewReadOperation.setTapAdvanceFilter(filter);

			previewReadOperationQueue.addOperation(sourceNode.getId(), previewReadOperation);
			PreviewFinishReadOperation previewFinishReadOperation = new PreviewFinishReadOperation();
			previewFinishReadOperation.last(true);
			previewReadOperationQueue.addOperation(sourceNode.getId(), previewFinishReadOperation);
		}
	}
}
