package io.tapdata.flow.engine.V2.util;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.FieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author samuel
 * @Description
 * @create 2025-05-14 11:38
 **/
@DisplayName("Class GraphUtil Test")
class GraphUtilTest {
	@Nested
	@DisplayName("Method findNodes test")
	class findNodesTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			TaskDto taskDto = new TaskDto();
			List<Node> nodes = new ArrayList<>();
			nodes.add(new TableNode() {{
				setId("1");
			}});
			nodes.add(new FieldRenameProcessorNode() {{
				setId("2");
			}});
			nodes.add(new MergeTableNode() {{
				setId("3");
			}});
			nodes.add(new TableNode() {{
				setId("4");
			}});
			List<Edge> edges = new ArrayList<>();
			edges.add(new Edge("1", "2"));
			edges.add(new Edge("2", "3"));
			edges.add(new Edge("3", "4"));
			taskDto.setDag(DAG.build(new Dag(edges, nodes)));
			List<Node> result = assertDoesNotThrow(() -> GraphUtil.findNodes(taskDto, TableNode.class, FieldRenameProcessorNode.class));
			assertEquals(3, result.size());
			assertSame(nodes.get(0), result.get(0));
			assertSame(nodes.get(1), result.get(1));
			assertSame(nodes.get(3), result.get(2));
		}

		@Test
		@DisplayName("test input task is null")
		void test2() {
			List<Node> result = assertDoesNotThrow(() -> GraphUtil.findNodes(null, TableNode.class));
			assertNotNull(result);
			assertTrue(result.isEmpty());
		}

		@Test
		@DisplayName("test nodes is null or empty")
		void test3() {
			TaskDto taskDto = new TaskDto();
			taskDto.setDag(DAG.build(new Dag()));
			List<Node> result = assertDoesNotThrow(() -> GraphUtil.findNodes(taskDto, TableNode.class));
			assertNotNull(result);
			assertTrue(result.isEmpty());

			taskDto.setDag(DAG.build(new Dag(new ArrayList<>(), new ArrayList<>())));
			result = assertDoesNotThrow(() -> GraphUtil.findNodes(taskDto, TableNode.class));
			assertNotNull(result);
			assertTrue(result.isEmpty());
		}
	}
}