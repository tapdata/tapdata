package io.tapdata.flow.engine.V2.task.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HazelcastTaskClientTest {
    @Test
    void test_deduceSchema(){
        TaskDto taskDto = new TaskDto();
        DAG dag = mock(DAG.class);
        taskDto.setDag(dag);
        when(dag.getNodes()).thenReturn(new ArrayList<>());
        taskDto.setSyncType(TaskDto.SYNC_TYPE_DEDUCE_SCHEMA);
        HazelcastTaskClient hazelcastTaskClient = new HazelcastTaskClient(mock(Job.class),taskDto,mock(ClientMongoOperator.class),mock(ConfigurationCenter.class),mock(HazelcastInstance.class));
        Assertions.assertNull(ReflectionTestUtils.getField(hazelcastTaskClient,"autoRecovery"));
    }
    @Test
    void test_testRun(){
        TaskDto taskDto = new TaskDto();
        DAG dag = mock(DAG.class);
        taskDto.setDag(dag);
        when(dag.getNodes()).thenReturn(new ArrayList<>());
        taskDto.setSyncType(TaskDto.SYNC_TYPE_TEST_RUN);
        HazelcastTaskClient hazelcastTaskClient = new HazelcastTaskClient(mock(Job.class),taskDto,mock(ClientMongoOperator.class),mock(ConfigurationCenter.class),mock(HazelcastInstance.class));
        Assertions.assertNull(ReflectionTestUtils.getField(hazelcastTaskClient,"autoRecovery"));
    }
    @Test
    void test_migrate(){
        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        DAG dag = mock(DAG.class);
        taskDto.setDag(dag);
        when(dag.getNodes()).thenReturn(new ArrayList<>());
        taskDto.setSyncType(TaskDto.SYNC_TYPE_MIGRATE);
        HazelcastTaskClient hazelcastTaskClient = new HazelcastTaskClient(mock(Job.class),taskDto,mock(ClientMongoOperator.class),mock(ConfigurationCenter.class),mock(HazelcastInstance.class));
        Assertions.assertNotNull(ReflectionTestUtils.getField(hazelcastTaskClient,"autoRecovery"));
    }
}
