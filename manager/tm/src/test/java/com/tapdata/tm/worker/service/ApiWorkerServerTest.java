package com.tapdata.tm.worker.service;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.worker.dto.ApiServerInfo;
import com.tapdata.tm.worker.dto.ApiServerStatus;
import com.tapdata.tm.worker.entity.Worker;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ApiWorkerServerTest {
    ApiWorkerServer apiWorkerServer;
    MongoTemplate mongoOperations;
    String workerInfoJson = "{\n" +
            "      \"workers\": {\n" +
            "        \"11\": {\n" +
            "          \"oid\": \"68bb9330661d7713deee3a6b\",\n" +
            "          \"id\": 11,\n" +
            "          \"pid\": 39097,\n" +
            "          \"worker_status\": \"listening\",\n" +
            "          \"worker_start_time\": 1757323402512,\n" +
            "          \"metricValues\": {\n" +
            "            \"CpuUsage\": 0,\n" +
            "            \"HeapMemoryUsage\": 41549824,\n" +
            "            \"lastUpdateTime\": \"2025-09-08T10:00:15.057Z\"\n" +
            "          },\n" +
            "          \"name\": \"Worker-1\",\n" +
            "          \"sort\": 0\n" +
            "        },\n" +
            "        \"12\": {\n" +
            "          \"oid\": \"68bb9330661d7713deee3a6e\",\n" +
            "          \"id\": 12,\n" +
            "          \"pid\": 39125,\n" +
            "          \"worker_status\": \"listening\",\n" +
            "          \"worker_start_time\": 1757323403013,\n" +
            "          \"metricValues\": {\n" +
            "            \"CpuUsage\": 0,\n" +
            "            \"HeapMemoryUsage\": 41566208,\n" +
            "            \"lastUpdateTime\": \"2025-09-08T10:00:15.057Z\"\n" +
            "          },\n" +
            "          \"name\": \"Worker-2\",\n" +
            "          \"sort\": 1\n" +
            "        },\n" +
            "        \"13\": {\n" +
            "          \"oid\": \"68bb9330661d7713deee3a6f\",\n" +
            "          \"id\": 13,\n" +
            "          \"pid\": 39154,\n" +
            "          \"worker_status\": \"listening\",\n" +
            "          \"worker_start_time\": 1757323403510,\n" +
            "          \"metricValues\": {\n" +
            "            \"CpuUsage\": 0,\n" +
            "            \"HeapMemoryUsage\": 43630592,\n" +
            "            \"lastUpdateTime\": \"2025-09-08T10:00:15.057Z\"\n" +
            "          },\n" +
            "          \"name\": \"Worker-3\",\n" +
            "          \"sort\": 2\n" +
            "        },\n" +
            "        \"14\": {\n" +
            "          \"oid\": \"68bb9330661d7713deee3a70\",\n" +
            "          \"id\": 14,\n" +
            "          \"pid\": 39186,\n" +
            "          \"worker_status\": \"listening\",\n" +
            "          \"worker_start_time\": 1757323404023,\n" +
            "          \"metricValues\": {\n" +
            "            \"CpuUsage\": 0,\n" +
            "            \"HeapMemoryUsage\": 43728896,\n" +
            "            \"lastUpdateTime\": \"2025-09-08T10:00:15.057Z\"\n" +
            "          },\n" +
            "          \"name\": \"Worker-4\",\n" +
            "          \"sort\": 3\n" +
            "        },\n" +
            "        \"15\": {\n" +
            "          \"oid\": \"68bb9330661d7713deee3a71\",\n" +
            "          \"id\": 15,\n" +
            "          \"pid\": 39449,\n" +
            "          \"worker_status\": \"listening\",\n" +
            "          \"worker_start_time\": 1757323404571,\n" +
            "          \"metricValues\": {\n" +
            "            \"CpuUsage\": 0,\n" +
            "            \"HeapMemoryUsage\": 43794432,\n" +
            "            \"lastUpdateTime\": \"2025-09-08T10:00:15.057Z\"\n" +
            "          },\n" +
            "          \"name\": \"Worker-5\",\n" +
            "          \"sort\": 4\n" +
            "        },\n" +
            "        \"16\": {\n" +
            "          \"oid\": \"68bb9330661d7713deee3a72\",\n" +
            "          \"id\": 16,\n" +
            "          \"pid\": 39474,\n" +
            "          \"worker_status\": \"listening\",\n" +
            "          \"worker_start_time\": 1757323405085,\n" +
            "          \"metricValues\": {\n" +
            "            \"CpuUsage\": 0,\n" +
            "            \"HeapMemoryUsage\": 43646976,\n" +
            "            \"lastUpdateTime\": \"2025-09-08T10:00:15.057Z\"\n" +
            "          },\n" +
            "          \"name\": \"Worker-6\",\n" +
            "          \"sort\": 5\n" +
            "        },\n" +
            "        \"17\": {\n" +
            "          \"oid\": \"68bb9330661d7713deee3a73\",\n" +
            "          \"id\": 17,\n" +
            "          \"pid\": 39499,\n" +
            "          \"worker_status\": \"listening\",\n" +
            "          \"worker_start_time\": 1757323405588,\n" +
            "          \"metricValues\": {\n" +
            "            \"CpuUsage\": 0,\n" +
            "            \"HeapMemoryUsage\": 43237376,\n" +
            "            \"lastUpdateTime\": \"2025-09-08T10:00:15.057Z\"\n" +
            "          },\n" +
            "          \"name\": \"Worker-7\",\n" +
            "          \"sort\": 6\n" +
            "        },\n" +
            "        \"18\": {\n" +
            "          \"oid\": \"68bb9330661d7713deee3a74\",\n" +
            "          \"id\": 18,\n" +
            "          \"pid\": 39524,\n" +
            "          \"worker_status\": \"listening\",\n" +
            "          \"worker_start_time\": 1757323406093,\n" +
            "          \"metricValues\": {\n" +
            "            \"CpuUsage\": 0,\n" +
            "            \"HeapMemoryUsage\": 43433984,\n" +
            "            \"lastUpdateTime\": \"2025-09-08T10:00:15.057Z\"\n" +
            "          },\n" +
            "          \"name\": \"Worker-8\",\n" +
            "          \"sort\": 7\n" +
            "        },\n" +
            "        \"19\": {\n" +
            "          \"oid\": \"68bb9330661d7713deee3a75\",\n" +
            "          \"id\": 19,\n" +
            "          \"pid\": 39549,\n" +
            "          \"worker_status\": \"listening\",\n" +
            "          \"worker_start_time\": 1757323406599,\n" +
            "          \"metricValues\": {\n" +
            "            \"CpuUsage\": 0,\n" +
            "            \"HeapMemoryUsage\": 43270144,\n" +
            "            \"lastUpdateTime\": \"2025-09-08T10:00:15.057Z\"\n" +
            "          },\n" +
            "          \"name\": \"Worker-9\",\n" +
            "          \"sort\": 8\n" +
            "        },\n" +
            "        \"20\": {\n" +
            "          \"oid\": \"68bb9330661d7713deee3a76\",\n" +
            "          \"id\": 20,\n" +
            "          \"pid\": 39574,\n" +
            "          \"worker_status\": \"listening\",\n" +
            "          \"worker_start_time\": 1757323407111,\n" +
            "          \"metricValues\": {\n" +
            "            \"CpuUsage\": 0,\n" +
            "            \"HeapMemoryUsage\": 43319296,\n" +
            "            \"lastUpdateTime\": \"2025-09-08T10:00:15.057Z\"\n" +
            "          },\n" +
            "          \"name\": \"Worker-10\",\n" +
            "          \"sort\": 9\n" +
            "        }\n" +
            "      },\n" +
            "      \"worker_process_id\": 38486,\n" +
            "      \"worker_process_start_time\": 1757323393373,\n" +
            "      \"worker_process_end_time\": null,\n" +
            "      \"status\": \"running\",\n" +
            "      \"exit_code\": null,\n" +
            "      \"metricValues\": {\n" +
            "        \"HeapMemoryUsage\": 1144.875,\n" +
            "        \"CpuUsage\": 2.8,\n" +
            "        \"lastUpdateTime\": \"2025-09-08T10:00:15.057Z\"\n" +
            "      }\n" +
            "    }";

    @BeforeEach
    void init() {
        apiWorkerServer = new ApiWorkerServer();
        mongoOperations = mock(MongoTemplate.class);
        ReflectionTestUtils.setField(apiWorkerServer, "mongoOperations", mongoOperations);
    }

    @Nested
    class getWorkersTest {
        @Test
        void testNormal() {
            ApiServerStatus jsonObject = JSON.parseObject(workerInfoJson, ApiServerStatus.class);
            Worker serverInfo = new Worker();
            serverInfo.setProcessId(new ObjectId().toHexString());
            serverInfo.setWorkerStatus(jsonObject);
            when(mongoOperations.findOne(any(Query.class), any(Class.class), anyString())).thenReturn(serverInfo);
            ApiServerInfo id = apiWorkerServer.getWorkers("id");
            Assertions.assertNotNull(id);
        }

        @Test
        void testWorkerMap() {
            ApiServerStatus jsonObject = JSON.parseObject(workerInfoJson, ApiServerStatus.class);
            Worker serverInfo = new Worker();
            serverInfo.setProcessId(new ObjectId().toHexString());
            serverInfo.setWorkerStatus(jsonObject);
            Map<String, String> stringStringMap = apiWorkerServer.workerMap(serverInfo);
            Assertions.assertNotNull(stringStringMap);
        }

        @Test
        void testGetApiServerWorkerInfo() {
            ApiServerStatus jsonObject = JSON.parseObject(workerInfoJson, ApiServerStatus.class);
            List<Worker> server = new ArrayList<>();
            Worker serverInfo = new Worker();
            serverInfo.setProcessId(new ObjectId().toHexString());
            serverInfo.setWorkerStatus(jsonObject);
            server.add(serverInfo);
            server.add(null);
            when(mongoOperations.find(any(Query.class), any(Class.class), anyString())).thenReturn(server);
            List<ApiServerInfo> apiServerWorkerInfo = apiWorkerServer.getApiServerWorkerInfo();
            Assertions.assertNotNull(apiServerWorkerInfo);
            Assertions.assertEquals(1, apiServerWorkerInfo.size());
        }
        @Test
        void testGetApiServerWorkerInfoNotAnyWorker() {
            when(mongoOperations.find(any(Query.class), any(Class.class), anyString())).thenReturn(new ArrayList());
            List<ApiServerInfo> apiServerWorkerInfo = apiWorkerServer.getApiServerWorkerInfo();
            Assertions.assertNotNull(apiServerWorkerInfo);
            Assertions.assertEquals(0, apiServerWorkerInfo.size());
        }
    }
}