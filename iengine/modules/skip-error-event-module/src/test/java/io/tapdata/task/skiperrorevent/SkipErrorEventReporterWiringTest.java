package io.tapdata.task.skiperrorevent;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.entity.logger.Log;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

class SkipErrorEventReporterWiringTest {

    @Test
    void fallsBackToTheEngineOperatorWhenSpringBeanIsNotAvailable() {
        ClientMongoOperator previousOperator = ConnectorConstant.clientMongoOperator;
        var previousContext = BeanUtil.configurableApplicationContext;
        SkipErrorEventAspectTask task = new SkipErrorEventAspectTask();
        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        taskDto.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        taskDto.setStatus(TaskDto.STATUS_RUNNING);
        TaskDto.SkipErrorEvent config = new TaskDto.SkipErrorEvent();
        config.setErrorMode(TaskDto.SkipErrorEvent.ErrorMode.DQL);
        taskDto.setSkipErrorEvent(config);
        task.setTask(taskDto);
        task.setLog(mock(Log.class));
        HttpClientMongoOperator engineOperator = mock(HttpClientMongoOperator.class);

        try {
            ConnectorConstant.clientMongoOperator = engineOperator;
            BeanUtil.configurableApplicationContext = null;
            task.onStart(new TaskStartAspect().task(taskDto));

            assertNotNull(ReflectionTestUtils.getField(task, "dqlEventReporter"));
        } finally {
            try {
                task.onStop(new TaskStopAspect().task(taskDto));
            } catch (Exception ignored) {
            }
            ConnectorConstant.clientMongoOperator = previousOperator;
            BeanUtil.configurableApplicationContext = previousContext;
        }
    }
}
