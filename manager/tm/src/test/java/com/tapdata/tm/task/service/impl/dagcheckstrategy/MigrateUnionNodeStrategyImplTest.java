package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.MigrateUnionProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import io.github.openlg.graphlib.Graph;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/10/15 18:09
 */
public class MigrateUnionNodeStrategyImplTest {

    @Test
    public void testGetLog() {
        Graph<Node, Edge> graph = new Graph<>();
        DatabaseNode srcNode = new DatabaseNode();
        srcNode.setId("1");
        srcNode.setSyncSourcePartitionTableEnable(true);
        graph.setNode("1", srcNode);

        MigrateUnionProcessorNode unionNode = new MigrateUnionProcessorNode();
        unionNode.setId("2");
        unionNode.setName("migrate union node");
        graph.setNode("2", unionNode);
        DatabaseNode targetNode = new DatabaseNode();
        targetNode.setId("3");
        graph.setNode("3", targetNode);
        graph.setEdge(new io.github.openlg.graphlib.Edge("1", "2"));
        graph.setEdge(new io.github.openlg.graphlib.Edge("2", "3"));

        DAG dag = new DAG(graph);
        srcNode.setDag(dag);
        unionNode.setDag(dag);
        targetNode.setDag(dag);

        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        //taskDto.setDag(dag);

        MigrateUnionNodeStrategyImpl unionNodeStrategy = new MigrateUnionNodeStrategyImpl();
        UserDetail userDetail = mock(UserDetail.class);
        when(userDetail.getUserId()).thenReturn("userId");
        List<TaskDagCheckLog> result = unionNodeStrategy.getLogs(taskDto, userDetail, Locale.CHINA);
        Assertions.assertNull(result);

        taskDto.setDag(dag);

        result = unionNodeStrategy.getLogs(taskDto, userDetail, Locale.CHINA);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.stream().anyMatch(r -> "MIGRATE_UNION_NODE_CHECK".equals(r.getCheckType()) && Level.ERROR == r.getGrade() ));
        System.out.println(JsonUtil.toJson(result));
        srcNode.setSyncSourcePartitionTableEnable(false);

        result = unionNodeStrategy.getLogs(taskDto, userDetail, Locale.CHINA);
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.stream().anyMatch(r -> "MIGRATE_UNION_NODE_CHECK".equals(r.getCheckType()) && Level.INFO == r.getGrade()));

        System.out.println(JsonUtil.toJson(result));
    }

}
