package com.tapdata.tm.cluster.service;

import cn.hutool.core.map.MapUtil;
import cn.hutool.json.JSONUtil;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.cluster.dto.UpdateAgentVersionParam;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ClusterStateServiceTest extends BaseJunit {

    @Autowired
    ClusterStateService clusterStateService;

    @Test
    void beforeSave() {
    }

    @Test
    void updateAgent() {
        UpdateAgentVersionParam updateAgentVersionParam=new UpdateAgentVersionParam();
        updateAgentVersionParam.setVersion("1.7.0");
        updateAgentVersionParam.setDownloadUrl("asda");
        updateAgentVersionParam.setToken("adsasd");
        updateAgentVersionParam.setProcessId("61aec9400865be74694dab6f-1fm9c494t");
        clusterStateService.updateAgent(updateAgentVersionParam,getUser("61407a8cfa67f20019f68f9f"));
    }

    @Test
    void statusInfo() {
        String s="{\n" +
                "    \"type\": \"statusInfo\",\n" +
                "    \"timestamp\": 1637049500734,\n" +
                "    \"data\": {\n" +
                "        \"systemInfo\": {\n" +
                "            \"hostname\": \"07443fefa7c0\",\n" +
                "            \"uuid\": \"48455891-5452-4bcb-bd36-a38c593deffd\",\n" +
                "            \"ip\": \"172.17.0.8\",\n" +
                "            \"ips\": [\n" +
                "                \"172.17.0.8\"\n" +
                "            ],\n" +
                "            \"time\": 1637049500734,\n" +
                "            \"accessCode\": \"a37815423260ab69d47bf07421397108\",\n" +
                "            \"username\": \"61407a6ed651da00114e58cc\",\n" +
                "            \"process_id\": \"61935c9684103d36ce972daa-1fkjq3ar4\",\n" +
                "            \"cpus\": 40,\n" +
                "            \"totalmem\": 92699336704,\n" +
                "            \"installationDirectory\": \"/opt/agent\",\n" +
                "            \"work_dir\": \"/opt/agent\",\n" +
                "            \"os\": \"docker\"\n" +
                "        },\n" +
                "        \"reportInterval\": 20000,\n" +
                "        \"engine\": {\n" +
                "            \"processID\": \" 84\",\n" +
                "            \"status\": \"running\"\n" +
                "        },\n" +
                "        \"management\": {\n" +
                "            \"processID\": \"\",\n" +
                "            \"status\": \"stopped\"\n" +
                "        },\n" +
                "        \"apiServer\": {\n" +
                "            \"processID\": \"\",\n" +
                "            \"status\": \"stopped\"\n" +
                "        },\n" +
                "        \"customMonitorStatus\": []\n" +
                "    },\n" +
                "    \"sign\": \"355fc9bf57c0821d2427d4d24c203c86\"\n" +
                "}";
        Map map=   JsonUtil.parseJson(s, Map.class);
        clusterStateService.statusInfo(map);
    }
}