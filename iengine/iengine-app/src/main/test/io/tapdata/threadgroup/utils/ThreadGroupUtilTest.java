package io.tapdata.threadgroup.utils;

import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.threadgroup.ConnectorOnTaskThreadGroup;
import io.tapdata.threadgroup.ProcessorOnTaskThreadGroup;
import io.tapdata.threadgroup.TaskThreadGroup;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadGroupUtilTest {

    @Test
    public void testGroupThreadGroup() {
        ThreadGroupUtil util = ThreadGroupUtil.create(TaskThreadGroup.class, new Class[]{ConnectorOnTaskThreadGroup.class, ProcessorOnTaskThreadGroup.class});
        TaskDto taskDto = new TaskDto();
        taskDto.setName("main-sub");
        AtomicInteger count = new AtomicInteger(0);
        ThreadGroup group = new TaskThreadGroup(taskDto);
        new Thread(group, () -> {
            TaskDto taskDto1 = new TaskDto();
            DatabaseNode node = new DatabaseNode();
            node.setName("test-node");
            DataProcessorContext context = new DataProcessorContext.DataProcessorContextBuilder().withTaskDto(taskDto1).withNode(node).build();
            ThreadGroup groupSub = new ProcessorOnTaskThreadGroup(context);
            new Thread(groupSub, () -> {
                count.incrementAndGet();
                while (count.get() >= 0) {
                    try {
                        Thread.sleep(100);
                    } catch (Exception e){}
                }
            }, "main-sub-1").start();

            synchronized (count) {
                try {
                    Map<ThreadGroup, Set<ThreadGroup>> groupRes = util.group(groupSub);
                    Assert.assertFalse(groupRes.isEmpty());
                    Assert.assertTrue(groupRes.containsKey(group));
                    Assert.assertEquals(1, groupRes.get(group).size());
                } finally {
                    count.set(-1);
                }
            }
        }, "main-sub").start();
        while (count.get() >= 0) {
            try {
                Thread.sleep(100);
            } catch (Exception e){}
        }
    }

}
