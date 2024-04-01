package io.tapdata.flow.engine.V2.node.hazelcast.data;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.mock;

public class HazelcastBaseNodeTest extends BaseHazelcastNodeTest {
    HazelcastBaseNode hazelcastBaseNode;
    @BeforeEach
    void beforeEach() {
        super.allSetup();
        hazelcastBaseNode = new HazelcastBaseNode(dataProcessorContext) {
        };
        ReflectionTestUtils.setField(hazelcastBaseNode,"clientMongoOperator",mock(HttpClientMongoOperator.class));
        ReflectionTestUtils.setField(hazelcastBaseNode,"obsLogger",mockObsLogger);
    }
    @Nested
    class ErrorHandleTest {

        @Test
        void testErrorHandle() {
            TapCodeException tapCodeException = new TapCodeException("");
            Assertions.assertEquals(tapCodeException,hazelcastBaseNode.errorHandle(tapCodeException,"test error"));
        }

        @Test
        void testErrorHandleSkip() {
            TapCodeException tapCodeException = new TapCodeException("");
            Assertions.assertNull(hazelcastBaseNode.errorHandle(tapCodeException,"error"));
        }
    }
}
