package io.tapdata.flow.engine.V2.exactlyonce;

import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.exception.TapExactlyOnceWriteExCode_22;
import io.tapdata.pdk.core.api.ConnectorNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class ExactlyOnceUtilTest {
    @DisplayName("")
    @Test
    void test1(){
        ConnectorNode connectorNode = mock(ConnectorNode.class);
        TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
            ExactlyOnceUtil.generateExactlyOnceTable(connectorNode);
        });
        assertEquals(TapExactlyOnceWriteExCode_22.TARGET_TYPES_GENERATOR_FAILED,tapCodeException.getCode());
    }
}
