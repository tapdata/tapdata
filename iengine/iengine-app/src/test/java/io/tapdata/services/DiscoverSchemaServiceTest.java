package io.tapdata.services;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import io.tapdata.Runnable.LoadSchemaRunner;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.pdk.PDKUtils;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.services.util.ConnectionNodeUtil;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;

class DiscoverSchemaServiceTest {

    @Test
    void testDiscoverSchemaPdkUtilsNull() {
        DiscoverSchemaService service = new DiscoverSchemaService();
        try (MockedStatic<InstanceFactory> instanceFactoryMockedStatic = Mockito.mockStatic(InstanceFactory.class)) {
            instanceFactoryMockedStatic.when(() -> InstanceFactory.instance(PDKUtils.class)).thenReturn(null);
            CoreException ex = assertThrows(CoreException.class, () -> service.discoverSchema("id", new HashMap<>()));
            assertEquals(NetErrors.ILLEGAL_PARAMETERS, ex.getCode());
        }
    }

    @Test
    void testDiscoverSchemaConnectionIdNull() {
        DiscoverSchemaService service = new DiscoverSchemaService();
        PDKUtils pdkUtils = mock(PDKUtils.class);
        try (MockedStatic<InstanceFactory> instanceFactoryMockedStatic = Mockito.mockStatic(InstanceFactory.class)) {
            instanceFactoryMockedStatic.when(() -> InstanceFactory.instance(PDKUtils.class)).thenReturn(pdkUtils);
            CoreException ex = assertThrows(CoreException.class, () -> service.discoverSchema(null, new HashMap<>()));
            assertEquals(NetErrors.ILLEGAL_PARAMETERS, ex.getCode());
        }
    }

    @Test
    void testDiscoverSchemaConnectionsNotFound() {
        DiscoverSchemaService service = new DiscoverSchemaService();
        PDKUtils pdkUtils = mock(PDKUtils.class);
        HazelcastTaskService taskService = mock(HazelcastTaskService.class);

        try (MockedStatic<InstanceFactory> instanceFactoryMockedStatic = Mockito.mockStatic(InstanceFactory.class);
             MockedStatic<HazelcastTaskService> hazelcastTaskServiceMockedStatic = Mockito.mockStatic(HazelcastTaskService.class)) {
            instanceFactoryMockedStatic.when(() -> InstanceFactory.instance(PDKUtils.class)).thenReturn(pdkUtils);
            hazelcastTaskServiceMockedStatic.when(HazelcastTaskService::taskService).thenReturn(taskService);
            when(taskService.getConnection("id")).thenReturn(null);

            CoreException ex = assertThrows(CoreException.class, () -> service.discoverSchema("id", new HashMap<>()));
            assertEquals(NetErrors.CONNECTIONS_NOT_FOUND, ex.getCode());
        }
    }

    @Test
    void testDiscoverSchemaNormal() {
        DiscoverSchemaService service = new DiscoverSchemaService();
        PDKUtils pdkUtils = mock(PDKUtils.class);
        PDKUtils.PDKInfo pdkInfo = mock(PDKUtils.PDKInfo.class);
        when(pdkUtils.downloadPdkFileIfNeed(anyString())).thenReturn(pdkInfo);

        HazelcastTaskService taskService = mock(HazelcastTaskService.class);
        Connections connections = mock(Connections.class);
        when(connections.getPdkHash()).thenReturn("hash");
        when(taskService.getConnection("id")).thenReturn(connections);

        ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);

        try (MockedStatic<InstanceFactory> instanceFactoryMockedStatic = Mockito.mockStatic(InstanceFactory.class);
             MockedStatic<HazelcastTaskService> hazelcastTaskServiceMockedStatic = Mockito.mockStatic(HazelcastTaskService.class);
             MockedStatic<BeanUtil> beanUtilMockedStatic = Mockito.mockStatic(BeanUtil.class);
             MockedConstruction<LoadSchemaRunner> loadSchemaRunnerConstruction = Mockito.mockConstruction(LoadSchemaRunner.class,
                     (mock, context) -> doNothing().when(mock).run())) {
            instanceFactoryMockedStatic.when(() -> InstanceFactory.instance(PDKUtils.class)).thenReturn(pdkUtils);
            hazelcastTaskServiceMockedStatic.when(HazelcastTaskService::taskService).thenReturn(taskService);
            beanUtilMockedStatic.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(clientMongoOperator);

            Assertions.assertDoesNotThrow(() -> service.discoverSchema("id", new HashMap<>()));
            assertEquals(1, loadSchemaRunnerConstruction.constructed().size());
            verify(loadSchemaRunnerConstruction.constructed().get(0), times(1)).run();
        }
    }

    @Test
    void testDiscoverSpecifySchemaReturnNullAndFirst() {
        DiscoverSchemaService service = spy(new DiscoverSchemaService());

        when(service.discoverSpecifySchemas(eq("c"), anyList())).thenReturn(new ArrayList<>());
        assertNull(service.discoverSpecifySchema("c", "t"));

        MetadataInstancesDto dto = new MetadataInstancesDto();
        when(service.discoverSpecifySchemas(eq("c"), anyList())).thenReturn(List.of(dto));
        assertEquals(dto, service.discoverSpecifySchema("c", "t"));
    }

    @Test
    void testDiscoverSpecifySchemasTablesEmpty() {
        DiscoverSchemaService service = new DiscoverSchemaService();
        List<MetadataInstancesDto> result = service.discoverSpecifySchemas("c", new ArrayList<>());
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    void testDiscoverSpecifySchemasNormalAndUpdateCalled() throws Throwable {
        DiscoverSchemaService service = new DiscoverSchemaService();

        ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
        when(clientMongoOperator.find(anyMap(), eq(ConnectorConstant.METADATA_INSTANCE_COLLECTION), eq(MetadataInstancesDto.class)))
                .thenReturn(List.of(new MetadataInstancesDto()));

        ConnectionNode connectionNode = mock(ConnectionNode.class, RETURNS_DEEP_STUBS);
        when(connectionNode.getConnectionContext().getSpecification().getDataTypesMap()).thenReturn(mock(DefaultExpressionMatchingMap.class));
        doNothing().when(connectionNode).connectorInit();
        doNothing().when(connectionNode).connectorStop();

        AtomicInteger updateCalls = new AtomicInteger(0);
        doAnswer(invocation -> {
            updateCalls.incrementAndGet();
            Query query = invocation.getArgument(0);
            Update update = invocation.getArgument(1);
            String collection = invocation.getArgument(2);
            assertEquals(ConnectorConstant.CONNECTION_COLLECTION + "/module", collection);
            Document q = query.getQueryObject();
            assertNotNull(q.get("_id"));
            Document u = update.getUpdateObject();
            assertNotNull(u.get("$set", Document.class));
            return null;
        }).when(clientMongoOperator).update(any(Query.class), any(Update.class), anyString());

        try (MockedStatic<BeanUtil> beanUtilMockedStatic = Mockito.mockStatic(BeanUtil.class);
             MockedStatic<ConnectionNodeUtil> connectionNodeUtilMockedStatic = Mockito.mockStatic(ConnectionNodeUtil.class);
             MockedStatic<LoadSchemaRunner> loadSchemaRunnerMockedStatic = Mockito.mockStatic(LoadSchemaRunner.class);
             MockedStatic<PDKInvocationMonitor> monitorMockedStatic = Mockito.mockStatic(PDKInvocationMonitor.class);
             MockedStatic<PDKIntegration> pdkIntegrationMockedStatic = Mockito.mockStatic(PDKIntegration.class)) {
            beanUtilMockedStatic.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(clientMongoOperator);
            connectionNodeUtilMockedStatic.when(() -> ConnectionNodeUtil.createConnectionNode(eq(clientMongoOperator), eq("c"), anyString())).thenReturn(connectionNode);

            loadSchemaRunnerMockedStatic.when(() -> LoadSchemaRunner.consumeTapTable(any(), any(), any())).thenAnswer(invocation -> {
                Consumer<TapTable> consumer = invocation.getArgument(0);
                List<TapTable> tables = invocation.getArgument(2);
                if (tables != null) {
                    for (TapTable t : tables) {
                        consumer.accept(t);
                    }
                }
                return null;
            });

            monitorMockedStatic.when(() -> PDKInvocationMonitor.invoke(any(), eq(PDKMethod.INIT), any(CommonUtils.AnyError.class), anyString(), anyString()))
                    .thenAnswer(invocation -> {
                        CommonUtils.AnyError r = invocation.getArgument(2);
                        try {
                            r.run();
                        } catch (Throwable t) {
                            throw new RuntimeException(t);
                        }
                        return null;
                    });
            monitorMockedStatic.when(() -> PDKInvocationMonitor.invoke(any(), eq(PDKMethod.STOP), any(CommonUtils.AnyError.class), anyString(), anyString()))
                    .thenAnswer(invocation -> {
                        CommonUtils.AnyError r = invocation.getArgument(2);
                        try {
                            r.run();
                        } catch (Throwable t) {
                            throw new RuntimeException(t);
                        }
                        return null;
                    });
            monitorMockedStatic.when(() -> PDKInvocationMonitor.invoke(any(), eq(PDKMethod.DISCOVER_SCHEMA), any(CommonUtils.AnyError.class), anyString()))
                    .thenAnswer(invocation -> {
                        CommonUtils.AnyError r = invocation.getArgument(2);
                        try {
                            r.run();
                        } catch (Throwable t) {
                            throw new RuntimeException(t);
                        }
                        return null;
                    });

            Class<?> connectorType;
            try {
                connectorType = ConnectionNode.class.getMethod("getConnector").getReturnType();
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
            Object connectorProxy = Proxy.newProxyInstance(
                    connectorType.getClassLoader(),
                    new Class[]{connectorType},
                    (proxy, method, args) -> {
                        if ("discoverSchema".equals(method.getName())) {
                            @SuppressWarnings("unchecked")
                            Consumer<List<TapTable>> consumer = (Consumer<List<TapTable>>) args[3];
                            consumer.accept(new ArrayList<>());
                            consumer.accept(List.of(new TapTable("t1")));
                        }
                        return null;
                    }
            );
            Mockito.doReturn(connectorProxy).when(connectionNode).getConnector();

            pdkIntegrationMockedStatic.when(() -> PDKIntegration.releaseAssociateId(anyString())).thenAnswer(inv -> null);

            List<MetadataInstancesDto> result = service.discoverSpecifySchemas("c", List.of("t1"));
            assertNotNull(result);
            assertEquals(1, result.size());
            Assertions.assertTrue(updateCalls.get() >= 1);
        }
    }

    @Test
    void testDiscoverSpecifySchemasCatchAndFinally() throws Throwable {
        DiscoverSchemaService service = new DiscoverSchemaService();
        ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
        ConnectionNode connectionNode = mock(ConnectionNode.class);

        try (MockedStatic<BeanUtil> beanUtilMockedStatic = Mockito.mockStatic(BeanUtil.class);
             MockedStatic<ConnectionNodeUtil> connectionNodeUtilMockedStatic = Mockito.mockStatic(ConnectionNodeUtil.class);
             MockedStatic<PDKInvocationMonitor> monitorMockedStatic = Mockito.mockStatic(PDKInvocationMonitor.class);
             MockedStatic<PDKIntegration> pdkIntegrationMockedStatic = Mockito.mockStatic(PDKIntegration.class)) {
            beanUtilMockedStatic.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(clientMongoOperator);
            connectionNodeUtilMockedStatic.when(() -> ConnectionNodeUtil.createConnectionNode(eq(clientMongoOperator), eq("c"), anyString())).thenReturn(connectionNode);

            when(connectionNode.getConnectionContext()).thenThrow(new RuntimeException("boom"));
            doNothing().when(connectionNode).connectorStop();

            monitorMockedStatic.when(() -> PDKInvocationMonitor.invoke(any(), eq(PDKMethod.STOP), any(CommonUtils.AnyError.class), anyString(), anyString()))
                    .thenAnswer(invocation -> {
                        CommonUtils.AnyError r = invocation.getArgument(2);
                        try {
                            r.run();
                        } catch (Throwable t) {
                            throw new RuntimeException(t);
                        }
                        return null;
                    });

            AtomicInteger releaseCalls = new AtomicInteger(0);
            pdkIntegrationMockedStatic.when(() -> PDKIntegration.releaseAssociateId(anyString())).thenAnswer(invocation -> {
                releaseCalls.incrementAndGet();
                return null;
            });

            CoreException ex = assertThrows(CoreException.class, () -> service.discoverSpecifySchemas("c", List.of("t1")));
            assertEquals(NetErrors.ILLEGAL_STATE, ex.getCode());
            assertEquals(1, releaseCalls.get());
        }
    }
}
