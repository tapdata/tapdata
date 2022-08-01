package io.tapdata.connector.redis;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.connector.constant.DeployModeEnum;
import io.tapdata.connector.constant.HostPort;
import io.tapdata.constant.DbTestItem;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.util.NetUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.List;

import static io.tapdata.base.ConnectorBase.testItem;

public class RedisTest {

    private final static String PING_RES = "PONG";

    private  RedisConfig redisConfig;

    private  RedisContext redisContext;

    public RedisTest(RedisConfig redisConfig) throws Exception {
        this.redisConfig = redisConfig;
        this.redisContext = new RedisContext(redisConfig);
    }

    public TestItem testHostPort() {
        try {
            DeployModeEnum deployModeEnum = DeployModeEnum.fromString(redisConfig.getDeployMode());
            if (deployModeEnum == null) {
                deployModeEnum = DeployModeEnum.STANDALONE;
            }
            if (deployModeEnum == DeployModeEnum.STANDALONE) {
                NetUtil.validateHostPortWithSocket(redisConfig.getHost(), redisConfig.getPort());
            } else {
                final List<HostPort> hostPorts = redisConfig.getSentinelAddress();
                if (CollectionUtils.isEmpty(hostPorts)) {
                    return testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, "host/port cannot be empty.");
                }

                StringBuilder failedHostPort = new StringBuilder();
                for (HostPort hostPort : hostPorts) {
                    try {
                        NetUtil.validateHostPortWithSocket(hostPort.getHost(), hostPort.getPort());
                    } catch (Exception e) {
                        failedHostPort.append(hostPort.getHost()).append(":").append(hostPort).append(",");
                    }
                }
                if (StringUtils.isNotBlank(failedHostPort)) {
                    return testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, JSONObject.toJSONString(failedHostPort));
                }
            }
        }catch (IOException e) {
            return testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, e.getMessage());
        }
        return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
    }

    public TestItem testConnect() {
        try {
            if (PING_RES.equalsIgnoreCase(redisContext.getJedis().ping())) {
                return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
            }
        } catch (Exception e) {
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
        }
        return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Elasticsearch client ping failed!");
    }

    public void close() {
        try {
            redisContext.close();
        } catch (Exception ignored) {
        }
    }



}
