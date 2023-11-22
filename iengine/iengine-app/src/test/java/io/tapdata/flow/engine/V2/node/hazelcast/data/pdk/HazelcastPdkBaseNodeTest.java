package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.tm.commons.task.dto.TaskDto;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Update;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HazelcastPdkBaseNodeTest {
    HazelcastPdkBaseNode hazelcastPdkBaseNode;

    @BeforeEach
    void init() {
        hazelcastPdkBaseNode = mock(HazelcastPdkBaseNode.class);
    }

    @Nested
    class FunctionRetryQueryTest {
        long timestamp;
        @BeforeEach
        void init() {
            timestamp = System.currentTimeMillis();
            when(hazelcastPdkBaseNode.functionRetryQuery(anyLong(), anyBoolean())).thenCallRealMethod();
        }

        /**signFunction*/
        @Test
        void functionRetryQueryNormal() {
            Update update = hazelcastPdkBaseNode.functionRetryQuery(timestamp, true);
            Document document = assertResult(update);
            Assertions.assertTrue(document.containsKey("taskRetryStartTime"));
            Assertions.assertEquals(timestamp, document.get("taskRetryStartTime"));
        }

        /**clearFunction*/
        @Test
        void functionRetryQueryNotSingle() {
            Update update = hazelcastPdkBaseNode.functionRetryQuery(timestamp, false);
            Document document = assertResult(update);
            Assertions.assertTrue(document.containsKey("functionRetryEx"));
            Assertions.assertEquals(timestamp + 5 * 60 * 1000L, document.get("functionRetryEx"));
            Assertions.assertTrue(document.containsKey("taskRetryStartTime"));
            Assertions.assertEquals(0, document.get("taskRetryStartTime"));
        }

        Document assertResult(Update update) {
            Assertions.assertNotNull(update);
            Document document = update.getUpdateObject();
            Assertions.assertNotNull(document);
            Assertions.assertTrue(document.containsKey("$set"));
            Object set = document.get("$set");
            Assertions.assertEquals(Document.class, set.getClass());
            Document setMap = (Document) set;
            Assertions.assertTrue(setMap.containsKey("functionRetryStatus"));
            Assertions.assertEquals(TaskDto.RETRY_STATUS_RUNNING, setMap.get("functionRetryStatus"));
            return setMap;
        }
    }

}
