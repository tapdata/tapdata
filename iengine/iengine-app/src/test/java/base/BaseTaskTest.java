package base;

import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.MockTaskUtil;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author samuel
 * @Description
 * @create 2023-11-22 11:26
 **/
public abstract class BaseTaskTest extends BaseTest {
	protected ProcessorBaseContext processorBaseContext;
	protected DataProcessorContext dataProcessorContext;
	protected TableNode tableNode;
	protected TaskDto taskDto;

	protected void allSetup() {
		setupTaskAndNode();
		setupContext();
	}

	protected void setupTaskAndNode() {
		// Mock task and node data
		taskDto = MockTaskUtil.setUpTaskDtoByJsonFile();
		tableNode = (TableNode) taskDto.getDag().getNodes().get(0);
	}

	protected void setupContext() {
		processorBaseContext = mock(ProcessorBaseContext.class);
		when(processorBaseContext.getTaskDto()).thenReturn(taskDto);
		when(processorBaseContext.getNode()).thenReturn((Node) tableNode);
		when(processorBaseContext.getEdges()).thenReturn(taskDto.getDag().getEdges());
		when(processorBaseContext.getNodes()).thenReturn(taskDto.getDag().getNodes());

		dataProcessorContext = mock(DataProcessorContext.class);
		when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
		when(dataProcessorContext.getNode()).thenReturn((Node) tableNode);
		when(dataProcessorContext.getEdges()).thenReturn(taskDto.getDag().getEdges());
		when(dataProcessorContext.getNodes()).thenReturn(taskDto.getDag().getNodes());
	}
}
