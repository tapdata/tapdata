package io.tapdata.flow.engine.V2.script;

import io.tapdata.pdk.core.api.ConnectorNode;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PdkNodeResourcesTest {

    @Test
    void connectorNodeUsesAtomicReferenceForThreadSafeAccess() throws NoSuchFieldException {
        Field connectorNode = PdkNodeResources.class.getDeclaredField("connectorNode");

        assertEquals(AtomicReference.class, connectorNode.getType());
        ParameterizedType connectorNodeType = (ParameterizedType) connectorNode.getGenericType();
        assertEquals(ConnectorNode.class, connectorNodeType.getActualTypeArguments()[0]);
    }
}
