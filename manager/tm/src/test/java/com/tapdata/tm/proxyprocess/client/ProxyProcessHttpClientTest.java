package com.tapdata.tm.proxyprocess.client;

import com.tapdata.tm.proxyprocess.dto.ProxyProcessCommandResult;
import com.tapdata.tm.proxyprocess.dto.ProxyProcessHttpRequest;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

class ProxyProcessHttpClientTest {
    @Test
    void shouldForwardBearerTokenAndReturnSynchronousResponse() {
        RestTemplate restTemplate = new RestTemplate();
        MockRestServiceServer server = MockRestServiceServer.bindTo(restTemplate).build();
        server.expect(requestTo("http://10.0.0.1:8080/api/db2-z/cdc/start?slot=cdc-1"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(header("Authorization", "Bearer agent-token"))
                .andRespond(withSuccess("{\"accepted\":true}", MediaType.APPLICATION_JSON));
        ProxyProcessHttpClient client = new ProxyProcessHttpClient(restTemplate);
        ProxyProcessHttpRequest request = new ProxyProcessHttpRequest();
        request.setMethod("POST");
        request.setPath("/api/db2-z/cdc/start");
        request.setQuery(Map.of("slot", "cdc-1"));
        request.setBody(Map.of("checkpoint", 10));

        ProxyProcessCommandResult result = client.execute(
                "http://10.0.0.1:8080/api/db2-z/cdc/start?slot=cdc-1", "agent-token", request);

        assertEquals(200, result.getStatusCode());
        assertEquals("{\"accepted\":true}", result.getBody());
        server.verify();
    }
}
