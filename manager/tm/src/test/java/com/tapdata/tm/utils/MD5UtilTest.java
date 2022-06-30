package com.tapdata.tm.utils;

import cn.hutool.crypto.digest.MD5;
import org.apache.commons.codec.digest.Md5Crypt;
import org.junit.jupiter.api.Test;
import org.springframework.util.DigestUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.*;

class MD5UtilTest {

    @Test
    void getMD5Str() {
        String s = "{\n" +
                "    \"type\": \"statusInfo\",\n" +
                "    \"timestamp\": 1636948891857,\n" +
                "    \"data\": {\n" +
                "        \"systemInfo\": {\n" +
                "            \"hostname\": \"DESKTOP-TLNP50P\",\n" +
                "            \"uuid\": \"f8c40fc6-59c9-4d3b-9f30-f027dbd9cefd\",\n" +
                "            \"ip\": \"192.168.3.65\",\n" +
                "            \"ips\": [\n" +
                "                \"192.168.3.65\"\n" +
                "            ],\n" +
                "            \"time\": 1636948891857,\n" +
                "            \"accessCode\": \"\",\n" +
                "            \"username\": \"\",\n" +
                "            \"process_id\": \"\",\n" +
                "            \"cpus\": 8,\n" +
                "            \"totalmem\": 16863318016,\n" +
                "            \"installationDirectory\": \"D:\\\\engin\",\n" +
                "            \"work_dir\": \"D:\\\\engin\",\n" +
                "            \"os\": \"win32\"\n" +
                "        },\n" +
                "        \"reportInterval\": 20000,\n" +
                "        \"engine\": {\n" +
                "            \"processID\": \"\",\n" +
                "            \"status\": \"stopped\"\n" +
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
                "    }\n" +
                "}";
        s = "tapdata" + s + "20200202";
        System.out.println("原文");
        System.out.println(s);
        System.out.println("密文");

    }

    @Test
    public void getMd5(){
        String s="{\n" +
                "    \"data\": {\n" +
                "        \"type\": \"update\",\n" +
                "        \"uuid\": \"f8c40fc6-59c9-4d3b-9f30-f027dbd9cefd\",\n" +
                "        \"process_id\": \"60ed7c3ad33e8d00122255cc-kr1zjk66\",\n" +
                "        \"operationTime\": \"Nov 13, 2021, 6:48:26 PM\",\n" +
                "        \"downloadUrl\": \"http://resource.tapdata.net/package/feagent/dfs-v1.0.3-071302-test-001/\",\n" +
                "        \"downloadList\": [\n" +
                "            \"log4j2.yml\",\n" +
                "            \"tapdata\"\n" +
                "        ],\n" +
                "        \"status\": 0,\n" +
                "        \"id\": {\n" +
                "            \"timestamp\": 1626184684,\n" +
                "            \"counter\": 2875114,\n" +
                "            \"randomValue1\": 14987991,\n" +
                "            \"randomValue2\": 18\n" +
                "        },\n" +
                "        \"createAt\": \"Jul 13, 2021, 9:58:04 PM\",\n" +
                "        \"lastUpdAt\": \"Nov 13, 2021, 6:48:58 PM\"\n" +
                "    },\n" +
                "    \"sign\": \"93167c07d445ee9756d34f04daca1e4c\",\n" +
                "    \"type\": \"update\",\n" +
                "    \"timestamp\": 1636945980055\n" +
                "}";

        s = "tapdata" + s + "20200202";
        System.out.println("原文");
        System.out.println(s);
        System.out.println("密文");
        System.out.println(MD5.create().digest(s));

    }

    @Test
    public void testMd5() throws NoSuchAlgorithmException {

        String s="tapdata{\"type\":\"statusInfo\",\"timestamp\":1636966508953,\"data\":{\"systemInfo\":{\"hostname\":\"DESKTOP-TLNP50P\",\"uuid\":\"f8c40fc6-59c9-4d3b-9f30-f027dbd9cefd\",\"ip\":\"192.168.3.65\",\"ips\":[\"192.168.3.65\"],\"time\":1636966508953,\"accessCode\":\"\",\"username\":\"\",\"process_id\":\"\",\"cpus\":8,\"totalmem\":16863318016,\"installationDirectory\":\"D:\\\\engin\",\"work_dir\":\"D:\\\\engin\",\"os\":\"win32\"},\"reportInterval\":20000,\"engine\":{\"processID\":\"\",\"status\":\"stopped\"},\"management\":{\"processID\":\"\",\"status\":\"stopped\"},\"apiServer\":{\"processID\":\"\",\"status\":\"stopped\"},\"customMonitorStatus\":[]}}20200202";
        System.out.println("原文");
        System.out.println(s);

        // e10adc3949ba59abbe56e057f20f883e
        String resultString1 = MD5Util.stringToMD5(s);
        System.out.println(resultString1);

    }


}