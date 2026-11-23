package io.tapdata.services;

import com.tapdata.entity.Connections;
import com.tapdata.tm.commons.inspect.CustomQueryRequest;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CustomQueryPreviewTimezoneTest {
    @Test void missingConfigDefaultsToUtcAndProducesTheResolvedFingerprint() {
        Connections connection = new Connections(); connection.setConfig(null);
        CustomQueryRequest request = new CustomQueryRequest();
        request.setConnectionId("connection"); request.setQuery("select 1");
        CustomQueryPreviewService.resolveTimezone(request, connection);
        assertEquals("UTC", request.getTimezone());
        String fingerprint = request.fingerprint();
        request.setTimezone(null);
        CustomQueryPreviewService.resolveTimezone(request, connection);
        assertEquals(fingerprint, request.fingerprint());
    }
    @Test void connectionTimezoneIsInheritedUnlessExplicitlyOverridden() {
        Connections connection = new Connections();
        connection.setConfig(Collections.singletonMap("timezone", "Asia/Shanghai"));
        CustomQueryRequest request = new CustomQueryRequest(); request.setTimezone(" ");
        CustomQueryPreviewService.resolveTimezone(request, connection);
        assertEquals("Asia/Shanghai", request.getTimezone());
        request.setTimezone("UTC"); CustomQueryPreviewService.resolveTimezone(request, connection);
        assertEquals("UTC", request.getTimezone());
        connection.setConfig(Collections.singletonMap("timezone", " ")); request.setTimezone(null);
        CustomQueryPreviewService.resolveTimezone(request, connection);
        assertEquals("UTC", request.getTimezone());
    }
}
