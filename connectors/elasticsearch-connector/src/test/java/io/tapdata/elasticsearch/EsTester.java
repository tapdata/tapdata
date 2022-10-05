package io.tapdata.elasticsearch;

import com.alibaba.fastjson.JSON;
import io.tapdata.elasticsearch.utils.Connector;
import io.tapdata.elasticsearch.utils.Indices;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/28 13:56 Create
 */
public class EsTester {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(EsTester.class);

    public static void main(String[] args) throws Exception {
        Connector.connect("192.168.1.189", 9204, client -> {
            try {
                info("version: {}", Connector.versionNumber(client));

                Indices.filterCallback(client, "hstest", (k, v) -> {
                    info("{}: {}", k, toJSONString(v));
                    return true;
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static String toJSONString(Object o) {
        return JSON.toJSONString(o);
    }

    private static void info(String msg, Object... args) {
        System.out.printf(msg.replaceAll("\\{}", "%s") + "\n", args);
    }

}
