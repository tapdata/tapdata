package io.tapdata.flow.engine.V2.node.hazelcast.data;

import com.alibaba.fastjson.JSON;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.dataflow.TableBatchReadStatus;
import com.tapdata.entity.dataflow.batch.BatchOffsetUtil;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.SyncTypeEnum;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

public class HazelcastTaskSourceTest {
    @Nested
    class InitJobOffsetTest{
        @Test
        void test1(){
            TaskDto taskDto=new TaskDto();
            taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
            Map<String,Object> attrs=new HashMap<>();
            taskDto.setAttrs(attrs);
            DataProcessorContext dataProcessorContext = DataProcessorContext.newBuilder().withTaskDto(taskDto).build();
            HazelcastTaskSource hazelcastTaskSource = mock(HazelcastTaskSource.class);
            ReflectionTestUtils.setField(hazelcastTaskSource,"dataProcessorContext",dataProcessorContext);
            doCallRealMethod().when(hazelcastTaskSource).initJobOffset();
            doCallRealMethod().when(hazelcastTaskSource).getDataProcessorContext();
            assertDoesNotThrow(()->{hazelcastTaskSource.initJobOffset();});
        }

    }
}
