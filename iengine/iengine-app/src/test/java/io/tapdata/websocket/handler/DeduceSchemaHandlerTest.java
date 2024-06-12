package io.tapdata.websocket.handler;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.ResponseBody;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.autoinspect.utils.GZIPUtil;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DmlPolicy;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.websocket.WebSocketEventResult;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class DeduceSchemaHandlerTest {

    @Nested
    class HandleTest {
        DeduceSchemaHandler deduceSchemaHandler;
        ResponseBody responseBody;
        Map<String, Object> event;
        ClientMongoOperator clientMongoOperator;
        @BeforeEach
        void before() {
            deduceSchemaHandler = new DeduceSchemaHandler();
            responseBody = new ResponseBody();
            event = new HashMap<>();
            clientMongoOperator = mock(ClientMongoOperator.class);
            ReflectionTestUtils.setField(deduceSchemaHandler, "clientMongoOperator", clientMongoOperator);
        }
        @Test
        void test(){
            DeduceSchemaHandler.DeduceSchemaRequest request = new DeduceSchemaHandler.DeduceSchemaRequest();
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            List<Node> nodeList = new ArrayList<>();
            LogCollectorNode logCollectorNode = new LogCollectorNode();
            List<String> ids = new ArrayList<>();
            ids.add("test");
            logCollectorNode.setId("source123");
            logCollectorNode.setConnectionIds(ids);
            TableNode tableNode2 = new TableNode();
            tableNode2.setId("target123");
            tableNode2.setDmlPolicy(new DmlPolicy());
            nodeList.add(tableNode2);
            nodeList.add(logCollectorNode);
            Dag dag = new Dag();
            Edge edge=new Edge("source123","target123");
            List<Edge> edges = Arrays.asList(edge);
            dag.setEdges(edges);
            dag.setNodes(nodeList);
            DAG mockDag =  DAG.build(dag);
            taskDto.setDag(mockDag);
            request.setTaskDto(taskDto);
            request.setMetadataInstancesDtoList(new ArrayList<>());
            request.setOptions(new DAG.Options());
            request.setDataSourceMap(new HashMap<>());
            request.setDefinitionDtoMap(new HashMap<>());
            request.setTransformerDtoMap(new HashMap<>());
            request.setUserId("userId");
            request.setUserName("name");
            String jsonResult = JsonUtil.toJsonUseJackson(request);
            byte[] gzip = GZIPUtil.gzip(jsonResult.getBytes());
            byte[] encode = Base64.getEncoder().encode(gzip);
            String dataString = new String(encode, StandardCharsets.UTF_8);
            event.put("data",dataString);
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            try(MockedStatic<ObsLoggerFactory> mockedStatic = mockStatic(ObsLoggerFactory.class);
                MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class)){
                mockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
                ConfigurationCenter configurationCenter = mock(ConfigurationCenter.class);
                beanUtilMockedStatic.when(()->BeanUtil.getBean(any())).thenReturn(configurationCenter);
                when(configurationCenter.getConfig(any())).thenReturn("agentId");
                WebSocketEventResult result = deduceSchemaHandler.handle(event);
                Assertions.assertTrue((Boolean) result.getResult());
            }


        }
    }
}
