package io.tapdata.flow.engine.V2.util;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/6/2 15:39 Create
 * @description
 */
public final class TaskAnalyze {
    private TaskAnalyze() {

    }

    public static int sourceNodeCount(TaskDto taskDto) {
        DAG dag = taskDto.getDag();
        int count = 0;
        for (Node<?> node : dag.getNodes()) {
            if (node instanceof TableNode || node instanceof DatabaseNode) {
                count++;
            }
        }
        //目标节点有且仅有1个
        return count - 1;
    }
}
