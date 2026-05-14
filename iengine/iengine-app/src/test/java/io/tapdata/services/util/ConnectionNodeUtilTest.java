package io.tapdata.services.util;

import com.tapdata.constant.ConnectionUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.entity.error.CoreException;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Answers.RETURNS_SELF;

class ConnectionNodeUtilTest {

    @Test
    void testConnectionNotFound() {
        HttpClientMongoOperator clientMongoOperator = mock(HttpClientMongoOperator.class);
        HazelcastTaskService taskService = mock(HazelcastTaskService.class);

        try (MockedStatic<HazelcastTaskService> hazelcastTaskServiceMockedStatic = Mockito.mockStatic(HazelcastTaskService.class)) {
            hazelcastTaskServiceMockedStatic.when(HazelcastTaskService::taskService).thenReturn(taskService);
            when(taskService.getConnection("connId")).thenReturn(null);

            assertThrows(CoreException.class, () -> ConnectionNodeUtil.createConnectionNode(clientMongoOperator, "connId", "a"));
        }
    }

    @Test
    void testDatabaseTypeNull() {
        HttpClientMongoOperator clientMongoOperator = mock(HttpClientMongoOperator.class);
        HazelcastTaskService taskService = mock(HazelcastTaskService.class);
        Connections connections = mock(Connections.class);
        when(connections.getPdkHash()).thenReturn("hash");

        try (MockedStatic<HazelcastTaskService> hazelcastTaskServiceMockedStatic = Mockito.mockStatic(HazelcastTaskService.class);
             MockedStatic<ConnectionUtil> connectionUtilMockedStatic = Mockito.mockStatic(ConnectionUtil.class)) {
            hazelcastTaskServiceMockedStatic.when(HazelcastTaskService::taskService).thenReturn(taskService);
            when(taskService.getConnection("connId")).thenReturn(connections);
            connectionUtilMockedStatic.when(() -> ConnectionUtil.getDatabaseType(any(), eq("hash"))).thenReturn(null);

            assertThrows(CoreException.class, () -> ConnectionNodeUtil.createConnectionNode(clientMongoOperator, "connId", "a"));
        }
    }

    @Test
    void testCreateConnectionNodeWithConfig() throws Exception {
        HttpClientMongoOperator clientMongoOperator = mock(HttpClientMongoOperator.class);
        HazelcastTaskService taskService = mock(HazelcastTaskService.class);

        Connections connections = mock(Connections.class);
        when(connections.getPdkHash()).thenReturn("hash");
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("k", "v");
        when(connections.getConfig()).thenReturn(cfg);

        DatabaseTypeEnum.DatabaseType databaseType = mock(DatabaseTypeEnum.DatabaseType.class);
        when(databaseType.getPdkHash()).thenReturn("pdkHash");
        when(databaseType.getJarFile()).thenReturn("jarFile");
        when(databaseType.getJarRid()).thenReturn("jarRid");
        when(databaseType.getGroup()).thenReturn("group");
        when(databaseType.getVersion()).thenReturn("version");
        when(databaseType.getPdkId()).thenReturn("pdkId");

        ConnectionNode node = mock(ConnectionNode.class);
        PDKIntegration.ConnectionConnectorBuilder builder = mock(PDKIntegration.ConnectionConnectorBuilder.class, RETURNS_SELF);
        when(builder.build()).thenReturn(node);

        try (MockedStatic<HazelcastTaskService> hazelcastTaskServiceMockedStatic = Mockito.mockStatic(HazelcastTaskService.class);
             MockedStatic<ConnectionUtil> connectionUtilMockedStatic = Mockito.mockStatic(ConnectionUtil.class);
             MockedStatic<PdkUtil> pdkUtilMockedStatic = Mockito.mockStatic(PdkUtil.class);
             MockedStatic<PDKIntegration> pdkIntegrationMockedStatic = Mockito.mockStatic(PDKIntegration.class)) {
            hazelcastTaskServiceMockedStatic.when(HazelcastTaskService::taskService).thenReturn(taskService);
            when(taskService.getConnection("connId")).thenReturn(connections);
            connectionUtilMockedStatic.when(() -> ConnectionUtil.getDatabaseType(any(), eq("hash"))).thenReturn(databaseType);
            pdkUtilMockedStatic.when(() -> PdkUtil.downloadPdkFileIfNeed(eq(clientMongoOperator), eq("pdkHash"), eq("jarFile"), eq("jarRid"))).thenAnswer(inv -> null);
            pdkIntegrationMockedStatic.when(PDKIntegration::createConnectionConnectorBuilder).thenReturn(builder);

            ConnectionNode result = ConnectionNodeUtil.createConnectionNode(clientMongoOperator, "connId", "associate");
            assertEquals(node, result);
            verify(builder, times(1)).withAssociateId("associate");
            verify(builder, times(1)).withGroup("group");
            verify(builder, times(1)).withVersion("version");
            verify(builder, times(1)).withPdkId("pdkId");
            verify(builder, times(1)).withConnectionConfig(any());
            verify(builder, times(1)).build();
        }
    }

    @Test
    void testCreateConnectionNodeWithoutConfig() throws Exception {
        HttpClientMongoOperator clientMongoOperator = mock(HttpClientMongoOperator.class);
        HazelcastTaskService taskService = mock(HazelcastTaskService.class);

        Connections connections = mock(Connections.class);
        when(connections.getPdkHash()).thenReturn("hash");
        when(connections.getConfig()).thenReturn(Collections.emptyMap());

        DatabaseTypeEnum.DatabaseType databaseType = mock(DatabaseTypeEnum.DatabaseType.class);
        when(databaseType.getPdkHash()).thenReturn("pdkHash");
        when(databaseType.getJarFile()).thenReturn("jarFile");
        when(databaseType.getJarRid()).thenReturn("jarRid");
        when(databaseType.getGroup()).thenReturn("group");
        when(databaseType.getVersion()).thenReturn("version");
        when(databaseType.getPdkId()).thenReturn("pdkId");

        ConnectionNode node = mock(ConnectionNode.class);
        PDKIntegration.ConnectionConnectorBuilder builder = mock(PDKIntegration.ConnectionConnectorBuilder.class, RETURNS_SELF);
        when(builder.build()).thenReturn(node);

        try (MockedStatic<HazelcastTaskService> hazelcastTaskServiceMockedStatic = Mockito.mockStatic(HazelcastTaskService.class);
             MockedStatic<ConnectionUtil> connectionUtilMockedStatic = Mockito.mockStatic(ConnectionUtil.class);
             MockedStatic<PdkUtil> pdkUtilMockedStatic = Mockito.mockStatic(PdkUtil.class);
             MockedStatic<PDKIntegration> pdkIntegrationMockedStatic = Mockito.mockStatic(PDKIntegration.class)) {
            hazelcastTaskServiceMockedStatic.when(HazelcastTaskService::taskService).thenReturn(taskService);
            when(taskService.getConnection("connId")).thenReturn(connections);
            connectionUtilMockedStatic.when(() -> ConnectionUtil.getDatabaseType(any(), eq("hash"))).thenReturn(databaseType);
            pdkUtilMockedStatic.when(() -> PdkUtil.downloadPdkFileIfNeed(eq(clientMongoOperator), anyString(), anyString(), anyString())).thenAnswer(inv -> null);
            pdkIntegrationMockedStatic.when(PDKIntegration::createConnectionConnectorBuilder).thenReturn(builder);

            ConnectionNode result = ConnectionNodeUtil.createConnectionNode(clientMongoOperator, "connId", "associate");
            assertEquals(node, result);
            verify(builder, never()).withConnectionConfig(any());
        }
    }

    @Test
    void testCreateConnectionNodeWrapException() throws Exception {
        HttpClientMongoOperator clientMongoOperator = mock(HttpClientMongoOperator.class);
        HazelcastTaskService taskService = mock(HazelcastTaskService.class);

        Connections connections = mock(Connections.class);
        when(connections.getPdkHash()).thenReturn("hash");
        when(connections.getConfig()).thenReturn(Collections.emptyMap());

        DatabaseTypeEnum.DatabaseType databaseType = mock(DatabaseTypeEnum.DatabaseType.class);
        when(databaseType.getPdkHash()).thenReturn("pdkHash");
        when(databaseType.getJarFile()).thenReturn("jarFile");
        when(databaseType.getJarRid()).thenReturn("jarRid");
        when(databaseType.getGroup()).thenReturn("group");
        when(databaseType.getVersion()).thenReturn("version");
        when(databaseType.getPdkId()).thenReturn("pdkId");

        PDKIntegration.ConnectionConnectorBuilder builder = mock(PDKIntegration.ConnectionConnectorBuilder.class, RETURNS_SELF);
        when(builder.build()).thenThrow(new RuntimeException("boom"));

        try (MockedStatic<HazelcastTaskService> hazelcastTaskServiceMockedStatic = Mockito.mockStatic(HazelcastTaskService.class);
             MockedStatic<ConnectionUtil> connectionUtilMockedStatic = Mockito.mockStatic(ConnectionUtil.class);
             MockedStatic<PdkUtil> pdkUtilMockedStatic = Mockito.mockStatic(PdkUtil.class);
             MockedStatic<PDKIntegration> pdkIntegrationMockedStatic = Mockito.mockStatic(PDKIntegration.class)) {
            hazelcastTaskServiceMockedStatic.when(HazelcastTaskService::taskService).thenReturn(taskService);
            when(taskService.getConnection("connId")).thenReturn(connections);
            connectionUtilMockedStatic.when(() -> ConnectionUtil.getDatabaseType(any(), eq("hash"))).thenReturn(databaseType);
            pdkUtilMockedStatic.when(() -> PdkUtil.downloadPdkFileIfNeed(eq(clientMongoOperator), anyString(), anyString(), anyString())).thenAnswer(inv -> null);
            pdkIntegrationMockedStatic.when(PDKIntegration::createConnectionConnectorBuilder).thenReturn(builder);

            RuntimeException ex = assertThrows(RuntimeException.class, () -> ConnectionNodeUtil.createConnectionNode(clientMongoOperator, "connId", "associate"));
            assertNotNull(ex.getMessage());
            Assertions.assertTrue(ex.getMessage().contains("Failed to create pdk connection node"));
            assertNotNull(ex.getCause());
        }
    }
}
