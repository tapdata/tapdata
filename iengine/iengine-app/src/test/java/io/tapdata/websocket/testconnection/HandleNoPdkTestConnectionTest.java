package io.tapdata.websocket.testconnection;

import com.tapdata.constant.OsUtil;
import com.tapdata.manager.common.utils.StringUtils;
import io.tapdata.websocket.SendMessage;
import io.tapdata.websocket.WebSocketEventResult;
import io.tapdata.websocket.handler.TestConnectionHandler;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class HandleNoPdkTestConnectionTest {

    @Test
    public void handleRocksdbTest() throws InterruptedException {

        TestConnectionHandler testConnectionHandler = new TestConnectionHandler();
        Map event = new HashMap();
        event.put("isExternalStorage",true);
        event.put("testType","rocksdb");
        Map config = new HashMap();
        config.put("uri","/data/test");
        event.put("config",config);

        SendMessage sendMessage = new SendMessage() {
            @Override
            public void send(WebSocketEventResult data) throws IOException {
                String tapdataWorkDir = System.getenv("TAPDATA_WORK_DIR");
                if (StringUtils.isBlank(tapdataWorkDir)) {
                    tapdataWorkDir = System.getProperty("user.dir");
                }
                String path = config.get("uri").toString();
                String  dir = tapdataWorkDir + path;
                if (OsUtil.isWindows()) {
                    dir = dir.replace("/", "\\");
                }
                File file = new File(dir);
                assertTrue(file.exists());
            }
        };
        testConnectionHandler.handle(event,sendMessage);
        Thread.sleep(4000L);
    }




}
