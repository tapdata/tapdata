package io.tapdata.flow.engine.V2.schedule;

import base.BaseTest;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TapDataTaskSchedulerUtilTest extends BaseTest {
    @Nested
    class SignTaskRetryWithTimestampTest {
        TaskDto taskDto;
        long timestamp;
        ClientMongoOperator mongoOperator;
        String taskId;
        @BeforeEach
        void init() {
            taskId = new ObjectId().toHexString();
            taskDto = mock(TaskDto.class);
            timestamp = System.currentTimeMillis();
            when(taskDto.getTaskRetryStartTimeFlag()).thenReturn(timestamp);
            mongoOperator = mock(ClientMongoOperator.class);
            when(mongoOperator.findOne(any(Query.class), anyString(), any(Class.class))).thenReturn(taskDto);
        }

        @Test
        void testSignTaskRetryWithTimestampNormal() {
            boolean withTimestamp = TapDataTaskSchedulerUtil.signTaskRetryWithTimestamp(taskId, mongoOperator);
            Assertions.assertFalse(withTimestamp);
        }
        @Test
        void testSignTaskRetryWithTimestampNullTaskId() {
            Assertions.assertTrue(TapDataTaskSchedulerUtil.signTaskRetryWithTimestamp(null, mongoOperator));
        }
        @Test
        void testSignTaskRetryWithTimestampNullClientMongoOperator() {
            Assertions.assertTrue(TapDataTaskSchedulerUtil.signTaskRetryWithTimestamp(taskId, null));
        }
    }

    @Nested
    class JudgeTaskRetryStartTimeTest {
        TaskDto taskDto;
        long timestamp;
        @BeforeEach
        void init() {
            taskDto = mock(TaskDto.class);
            timestamp = System.currentTimeMillis();
        }

        @Test
        void testJudgeTaskRetryStartTimeNormal() {
            when(taskDto.getTaskRetryStartTime()).thenReturn(timestamp);
            boolean needRetryStartTime = TapDataTaskSchedulerUtil.judgeTaskRetryStartTime(taskDto);
            Assertions.assertFalse(needRetryStartTime);
        }

        @Test
        void testJudgeTaskRetryStartTimeNullRetryStartTimeInTaskDto() {
            when(taskDto.getTaskRetryStartTime()).thenReturn(null);
            boolean needRetryStartTime = TapDataTaskSchedulerUtil.judgeTaskRetryStartTime(taskDto);
            Assertions.assertTrue(needRetryStartTime);
        }

        @Test
        void testJudgeTaskRetryStartTimeLessThanZeroRetryStartTimeInTaskDto() {
            when(taskDto.getTaskRetryStartTime()).thenReturn(-1L);
            boolean needRetryStartTime = TapDataTaskSchedulerUtil.judgeTaskRetryStartTime(taskDto);
            Assertions.assertTrue(needRetryStartTime);
        }

        @Test
        void testJudgeTaskRetryStartTimeEqualZeroRetryStartTimeInTaskDto() {
            when(taskDto.getTaskRetryStartTime()).thenReturn(0L);
            boolean needRetryStartTime = TapDataTaskSchedulerUtil.judgeTaskRetryStartTime(taskDto);
            Assertions.assertTrue(needRetryStartTime);
        }

        @Test
        void testJudgeTaskRetryStartTimeNullTaskDto() {
            Assertions.assertTrue(TapDataTaskSchedulerUtil.judgeTaskRetryStartTime(null));
        }
    }

    @Nested
    class SignTaskRetryQueryTest {
        String taskId;
        @Test
        void testSingTaskRetryQueryNormal() {
            taskId = new ObjectId().toHexString();
            Query query = TapDataTaskSchedulerUtil.signTaskRetryQuery(taskId);
            Assertions.assertNotNull(query);
            Document document = query.getQueryObject();
            Assertions.assertTrue(document.containsKey("_id"));
            Object id = document.get("_id");
            Assertions.assertNotNull(id);
            Assertions.assertEquals(taskId, id.toString());
        }

        @Test
        void testSingTaskRetryQueryNullTaskId() {
            Assertions.assertNull(TapDataTaskSchedulerUtil.signTaskRetryQuery(null));
        }
        @Test
        void testSingTaskRetryQueryNotObjectIdTaskId() {
            Assertions.assertThrows(IllegalArgumentException.class, () -> {
                TapDataTaskSchedulerUtil.signTaskRetryQuery("111");
            });
        }

    }

    @Nested
    class SignTaskRetryUpdate {
        long timestamp;
        @BeforeEach
        void init() {
            timestamp = System.currentTimeMillis();
        }

        /**signFunction*/
        @Test
        void functionRetryQueryNormal() {
            Update update = TapDataTaskSchedulerUtil.signTaskRetryUpdate(true, timestamp);
            Document document = assertResult(update);
            Assertions.assertTrue(document.containsKey("taskRetryStartTime"));
            Assertions.assertEquals(timestamp, document.get("taskRetryStartTime"));
        }

        /**clearFunction*/
        @Test
        void functionRetryQueryNotSingle() {
            Update update = TapDataTaskSchedulerUtil.signTaskRetryUpdate(false, timestamp);
            Document document = assertResult(update);
            Assertions.assertFalse(document.containsKey("taskRetryStartTime"));
        }

        Document assertResult(Update update) {
            Assertions.assertNotNull(update);
            Document document = update.getUpdateObject();
            Assertions.assertNotNull(document);
            Assertions.assertTrue(document.containsKey("$set"));
            Object set = document.get("$set");
            Assertions.assertEquals(Document.class, set.getClass());
            Document setMap = (Document) set;
            Assertions.assertTrue(setMap.containsKey("taskRetryStatus"));
            Assertions.assertEquals(TaskDto.RETRY_STATUS_RUNNING, setMap.get("taskRetryStatus"));
            return setMap;
        }
    }
}
