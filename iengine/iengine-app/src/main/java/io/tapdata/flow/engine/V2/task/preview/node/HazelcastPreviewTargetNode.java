package io.tapdata.flow.engine.V2.task.preview.node;

import com.hazelcast.jet.core.Inbox;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataPreviewCompleteEvent;
import com.tapdata.entity.task.config.TaskGlobalVariable;
import com.tapdata.entity.task.context.DataProcessorContext;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastDataBaseNode;
import io.tapdata.flow.engine.V2.task.preview.TaskPreviewService;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2024-09-29 14:50
 **/
public class HazelcastPreviewTargetNode extends HazelcastDataBaseNode {
	private static final String TAG = HazelcastPreviewTargetNode.class.getSimpleName();

	public HazelcastPreviewTargetNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws TapCodeException {
		super.doInit(context);
		Thread.currentThread().setName(String.join("_", TAG, dataProcessorContext.getTaskDto().getId().toString(), getNode().getId()));
		System.out.println("xxx init preview target node");
	}

	@Override
	public void process(int ordinal, @NotNull Inbox inbox) {
		if (inbox.isEmpty()) {
			return;
		}
		List<TapdataEvent> tapdataEvents = new ArrayList<>();
		int drained = inbox.drainTo(tapdataEvents);
		if (drained <= 0) {
			return;
		}
		for (TapdataEvent tapdataEvent : tapdataEvents) {
			System.out.println("xxx receive event: " + tapdataEvent);
			if (tapdataEvent instanceof TapdataPreviewCompleteEvent) {
				Map<String, Object> taskGlobalVariable = TaskGlobalVariable.INSTANCE
						.getTaskGlobalVariable(TaskPreviewService.taskPreviewInstanceId(dataProcessorContext.getTaskDto()));
				taskGlobalVariable.put(TaskGlobalVariable.PREVIEW_COMPLETE_KEY, true);
			}
		}
	}
}
