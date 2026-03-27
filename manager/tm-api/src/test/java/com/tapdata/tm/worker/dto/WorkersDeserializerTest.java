package com.tapdata.tm.worker.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class WorkersDeserializerTest {
    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper();

        SimpleModule module = new SimpleModule();
        module.addDeserializer(Map.class, new WorkersDeserializer());

        mapper.registerModule(module);
    }

    @Test
    void testWorkersAsMap() throws Exception {

        String json = """
        {
          "workerA": {"pid":2001},
          "workerB": {"pid":2002}
        }
        """;

        Map<String, ApiServerWorkerInfo> result =
                mapper.readValue(json,
                        mapper.getTypeFactory()
                                .constructMapType(Map.class, String.class, ApiServerWorkerInfo.class));

        assertEquals(2, result.size());

        assertEquals(2001, result.get("workerA").getPid());
        assertEquals(2002, result.get("workerB").getPid());
    }

    @Test
    void testWorkersNull() throws Exception {

        String json = "null";

        Map<String, ApiServerWorkerInfo> result =
                mapper.readValue(json,
                        mapper.getTypeFactory()
                                .constructMapType(Map.class, String.class, ApiServerWorkerInfo.class));

        assertNull(result);
    }

    @Test
    void testApiServerStatusWorkersList() throws Exception {

        String json = """
        {
          "workers": [
            {"pid":1001,"oid":"1002"},
            {"pid":1002,"oid":"1003"}
          ]
        }
        """;

        ApiServerStatus status = mapper.readValue(json, ApiServerStatus.class);

        assertEquals(2, status.getWorkers().size());
        assertEquals(1001, status.getWorkers().get("1002").getPid());
    }
}