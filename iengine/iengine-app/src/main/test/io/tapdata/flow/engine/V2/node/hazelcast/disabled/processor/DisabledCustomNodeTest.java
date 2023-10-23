package io.tapdata.flow.engine.V2.node.hazelcast.disabled.processor;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.CustomProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastCustomProcessor;
import io.tapdata.schema.TapTableMap;
import org.junit.Test;

public class DisabledCustomNodeTest {
    @Test
    public void testDisableParam(){
        TaskDto taskDto = new TaskDto();
        Node<?> node = new CustomProcessorNode();
        ConfigurationCenter config = new ConfigurationCenter();
        TapTableMap<String, TapTable> tapTableMap = TapTableMap.create("");
        TaskConfig taskConfig = new TaskConfig();
        HazelcastCustomProcessor hazelcastCustomProcessor = new HazelcastCustomProcessor(
                DataProcessorContext.newBuilder()
                        .withTaskDto(taskDto)
                        .withNode(node)
                        .withConfigurationCenter(config)
                        .withTapTableMap(tapTableMap)
                        .withTaskConfig(taskConfig)
                        .build()
        );

//        hazelcastCustomProcessor.
    }
}
