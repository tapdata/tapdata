package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.GetCurrentTimestampFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Date;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("HazelcastSourcePdkDataNode Class Test")
public class HazelcastSourcePdkDataNodeTest extends BaseHazelcastNodeTest {
    private HazelcastSourcePdkDataNode instance;
    private MockHazelcastSourcePdkBaseNode mockInstance;

    private HttpClientMongoOperator clientMongoOperator;;

    @BeforeEach
    void beforeEach() {
        super.allSetup();
        mockInstance = mock(MockHazelcastSourcePdkBaseNode.class);
        ReflectionTestUtils.setField(mockInstance, "processorBaseContext", processorBaseContext);
        ReflectionTestUtils.setField(mockInstance, "dataProcessorContext", dataProcessorContext);
        when(mockInstance.getDataProcessorContext()).thenReturn(dataProcessorContext);
        ReflectionTestUtils.setField(mockInstance, "obsLogger", mockObsLogger);
        instance = new HazelcastSourcePdkDataNode(dataProcessorContext) {

        };
        clientMongoOperator = Mockito.mock(HttpClientMongoOperator.class);
        ReflectionTestUtils.setField(instance,"clientMongoOperator",clientMongoOperator);
    }

    @Nested
    @DisplayName("doCdc methods test")
    class DoCdcTest {
        @Test
        void testDoCdc() {
            try(MockedStatic<AspectUtils> mockedStatic = Mockito.mockStatic(AspectUtils.class)){
                HazelcastSourcePdkDataNode spyInstance = Mockito.spy(instance);
                spyInstance.running.set(true);
                doNothing().when(spyInstance).enterCDCStage();
                doReturn(new ConnectorNode()).when(spyInstance).getConnectorNode();
                doReturn(PDKMethodInvoker.create()).when(spyInstance).createPdkMethodInvoker();
                doReturn(new StreamReadConsumer()).when(spyInstance).generateStreamReadConsumer(any(ConnectorNode.class),any(PDKMethodInvoker.class));
                TaskConfig taskConfig = TaskConfig.create();
                taskConfig.taskRetryConfig(TaskRetryConfig.create());
                taskConfig.getTaskRetryConfig().retryIntervalSecond(1000L);
                when(dataProcessorContext.getTaskConfig()).thenReturn(taskConfig);

                ConnectorNode connectorNode = new ConnectorNode();
                ConnectorFunctions connectorFunctions = new ConnectorFunctions();
                GetCurrentTimestampFunction getCurrentTimestampFunction = mock(GetCurrentTimestampFunction.class);
                connectorFunctions.supportGetCurrentTimestampFunction(getCurrentTimestampFunction);
                connectorNode.init(null,null,connectorFunctions);
                long time = new Date().getTime() - 2000L;
                when(getCurrentTimestampFunction.now(null)).thenReturn(time);
                doReturn(connectorNode).when(spyInstance).getConnectorNode();
                when(clientMongoOperator.update(any(Query.class),any(Update.class),anyString())).thenAnswer(invocationOnMock -> {
                    Update update = invocationOnMock.getArgument(1);
                    Document set = (Document) update.getUpdateObject().get("$set");
                    Long timeDifference = (Long)set.get("timeDifference");
                    Assertions.assertTrue(timeDifference > 1000);
                    return null;
                });
                doNothing().when(spyInstance).doNormalCDC();
                spyInstance.doCdc();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
        @Test
        void testDoCdc_failed() {
                HazelcastSourcePdkDataNode spyInstance = Mockito.spy(instance);
                spyInstance.doCdc();
                verify(spyInstance,times(0)).initSourceAndEngineTimeDifference();
        }
    }
}
