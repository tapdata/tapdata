package io.tapdata.node.pdk;

import com.tapdata.constant.ConnectionUtil;
import com.tapdata.entity.Connections;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.core.api.ConnectorNode;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ConnectorNodeServiceTest {
    @Test
    void sharedPreviewNodeHasSavedResourceIdBeforeLightInit() throws Throwable {
        Connections datasource = new Connections();
        datasource.setId("0123456789ABCDEF01234567");
        TapConnectorContext context = new TapConnectorContext(null, null, null, null);
        ConnectorNode node = mock(ConnectorNode.class);
        when(node.getConnectorContext()).thenReturn(context);
        AtomicReference<String> idAtInit = new AtomicReference<>();
        doAnswer(call -> {
            idAtInit.set(context.getInfo().getString(TapConnectorContext.INFO_RESOURCE_ID));
            return null;
        }).when(node).connectorLightInit();

        ConnectorNodeService service = ConnectorNodeService.getInstance();
        try (MockedStatic<ConnectionUtil> connections = mockStatic(ConnectionUtil.class);
             MockedStatic<PdkUtil> nodes = mockStatic(PdkUtil.class)) {
            connections.when(() -> ConnectionUtil.getConnection(datasource.getId(), null)).thenReturn(datasource);
            nodes.when(() -> PdkUtil.createNode(anyString(), any(), any(), anyString(), any(), any(), any(),
                    any(), any(), any(), any(), any(), any(), any())).thenReturn(node);
            nodes.when(() -> PdkUtil.populateConnectionInfo(context, datasource)).thenCallRealMethod();
            for (int lookup = 0; lookup < 2; lookup++) {
                service.globalConnectorNode(datasource.getId(), null, null, (result, error) -> {
                    assertNull(error);
                    assertSame(node, service.getConnectorNode(result.getAssociateId()));
                });
            }
            verify(node).connectorLightInit();
            assertEquals("0123456789abcdef01234567", idAtInit.get());
        } finally {
            service.removeConnectorNode(ConnectorNodeService.globalAssociateId(datasource));
        }
    }
}
