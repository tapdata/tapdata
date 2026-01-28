package com.tapdata.tm.v2.api.monitor.main.dto;

import com.tapdata.tm.worker.entity.Worker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ApiOfEachServerTest {

    @Test
    void testNormal() {
        List<ApiOfEachServer> apiMetricsRaws = new ArrayList<>();
        ApiOfEachServer item = new ApiOfEachServer();
        item.setServerId("serverId");
        apiMetricsRaws.add(item);

        ApiOfEachServer item1 = new ApiOfEachServer();
        item1.setServerId("serverId1");
        apiMetricsRaws.add(item1);


        Map<String, Worker> apiServerMap = new HashMap<>();
        Worker worker = new Worker();
        apiServerMap.put("serverId", worker);
        List<ApiOfEachServer> items = ApiOfEachServer.supplement(apiMetricsRaws, apiServerMap);
        Assertions.assertNotNull(items);
        Assertions.assertEquals(1, items.size());
    }
}