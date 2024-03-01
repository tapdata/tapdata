package io.tapdata.websocket.testconnection;

import com.tapdata.constant.OsUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.validator.ConnectionValidateResultDetail;
import io.tapdata.websocket.SendMessage;
import io.tapdata.websocket.WebSocketEventResult;
import io.tapdata.websocket.handler.TestConnectionHandler;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

class HandleNoPdkTestConnectionTest {

    @Test
    void handleRocksdbTest(){

        TestConnectionHandler testConnectionHandler = new TestConnectionHandler();
        Map event = new HashMap();
        event.put("isExternalStorage",true);
        event.put("testType","rocksdb");
        Map config = new HashMap();
        config.put("uri","/data/test");
        event.put("config",config);
        final String[] mark = {""};
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
                mark[0] = "return";
                File file = new File(dir);
                assertTrue(file.exists());
            }
        };
        testConnectionHandler.handle(event,sendMessage);
        await().atMost(15, TimeUnit.SECONDS).until(() -> mark[0] == "return");

    }



    @Test
    void handleOtherTypeTest() {

        TestConnectionHandler testConnectionHandler = new TestConnectionHandler();
        ClientMongoOperator clientMongoOperator = Mockito.mock(ClientMongoOperator.class);
        ReflectionTestUtils.setField(testConnectionHandler,"clientMongoOperator",clientMongoOperator);
        Map event = new HashMap();
        event.put("isExternalStorage",true);
        String testType = "rock";
        event.put("testType",testType);
        Map config = new HashMap();
        config.put("uri","/data/test");
        event.put("config",config);
        final String[] mark = {""};

        SendMessage sendMessage = new SendMessage() {
            @Override
            public void send(WebSocketEventResult data){
               String actualData =  data.getError();
                String errorAlarm = "TestType not found instance '" + testType + "'";
                mark[0] = "return";
                assertTrue(actualData.contains(errorAlarm));
            }


        };
        testConnectionHandler.handle(event,sendMessage);
        await().atMost(15, TimeUnit.SECONDS).until(() -> mark[0] == "return");

    }


    @Test
    void handlePdkTypeTest() {
        TestConnectionHandler testConnectionHandler = new TestConnectionHandler();
        ClientMongoOperator clientMongoOperator = Mockito.mock(ClientMongoOperator.class);
        ReflectionTestUtils.setField(testConnectionHandler,"clientMongoOperator",clientMongoOperator);
        Map event = new HashMap();
        String testType = "rock";
        event.put("testType",testType);
        Map config = new HashMap();
        config.put("uri","/data/test");
        event.put("config",config);
        event.put("pdkType","mongodb");
        final String[] mark = {""};
        SendMessage sendMessage = new SendMessage() {
            @Override
            public void send(WebSocketEventResult data){
                String actualData =  data.getError();
                String errorAlarm = "Unknown database type";
                mark[0] = "return";
                assertTrue(actualData.contains(errorAlarm));
            }

        };
        testConnectionHandler.handle(event,sendMessage);
        await().atMost(15, TimeUnit.SECONDS).until(() -> mark[0] == "return");


    }

    @Test
    void handleRocksdbWriteFileExistTest() throws IOException {
        RocksDBTestConnectionImpl rocksDBTestConnection = new RocksDBTestConnectionImpl();
        String tapdataWorkDir = System.getenv("TAPDATA_WORK_DIR");
        if (StringUtils.isBlank(tapdataWorkDir)) {
            tapdataWorkDir = System.getProperty("user.dir");
        }
        String  dir = tapdataWorkDir;
        if (OsUtil.isWindows()) {
            dir = dir.replace("/", "\\");
        }
        File file = new File(dir, "testConnect.txt");
        file.getParentFile().mkdirs();
        file.createNewFile();
        ConnectionValidateResultDetail connectionValidateResultDetail = rocksDBTestConnection.handleFileWrite(dir);
        assertEquals("passed",connectionValidateResultDetail.getStatus());
    }

    @Test
    void handleRocksdbReadFileExistTest() throws IOException {
        RocksDBTestConnectionImpl rocksDBTestConnection = new RocksDBTestConnectionImpl();
        String tapdataWorkDir = System.getenv("TAPDATA_WORK_DIR");
        if (StringUtils.isBlank(tapdataWorkDir)) {
            tapdataWorkDir = System.getProperty("user.dir");
        }
        String  dir = tapdataWorkDir;
        if (OsUtil.isWindows()) {
            dir = dir.replace("/", "\\");
        }
        File file = new File(dir, "testConnect.txt");
        file.getParentFile().mkdirs();
        file.createNewFile();
        ConnectionValidateResultDetail connectionValidateResultDetail = rocksDBTestConnection.handleFileRead(dir);
        assertEquals("passed",connectionValidateResultDetail.getStatus());
    }

    @Test
    void handleRocksdbWriteExceptionTest() throws IOException, InterruptedException {
        RocksDBTestConnectionImpl rocksDBTestConnection = new RocksDBTestConnectionImpl();
        String tapdataWorkDir = System.getenv("TAPDATA_WORK_DIR");
        if (StringUtils.isBlank(tapdataWorkDir)) {
            tapdataWorkDir = System.getProperty("user.dir");
        }
        String dir = tapdataWorkDir;
        if (OsUtil.isWindows()) {
            dir = dir.replace("/", "\\");
        }
        File file = new File(dir, "testConnect.txt");
        file.getParentFile().mkdirs();
        file.createNewFile();
        file.setWritable(false, false);
        if (OsUtil.isLinux()) {
            ProcessBuilder processBuilder = new ProcessBuilder("chmod", "555", file.getPath());
            int exitCode = processBuilder.start().waitFor();
            if (exitCode == 0) {
                System.out.println("文件权限已成功更新！");
            } else {
                System.err.println("无法更新文件权限。退出码为：" + exitCode);
            }
        }
        ConnectionValidateResultDetail connectionValidateResultDetail = rocksDBTestConnection.handleFileWrite(dir);
        file.setWritable(true);
        Files.delete(file.toPath());
        assertEquals("failed", connectionValidateResultDetail.getStatus());
    }


    @Test
    void handleRocksdbWriteIoExceptionTest() {
        RocksDBTestConnectionImpl rocksDBTestConnection = new RocksDBTestConnectionImpl();
        String toLongPathName = UUID.randomUUID().toString();
        for (int i = 0; i < 5; i++) toLongPathName += toLongPathName;
        ConnectionValidateResultDetail connectionValidateResultDetail = rocksDBTestConnection.handleFileWrite(toLongPathName);
        assertEquals("failed", connectionValidateResultDetail.getStatus());
    }

    @Test
    void handleRocksdbReadIoExceptionTest(){
        RocksDBTestConnectionImpl rocksDBTestConnection = new RocksDBTestConnectionImpl();
        String toLongPathName = UUID.randomUUID().toString();
        for (int i = 0; i < 5; i++) toLongPathName += toLongPathName;
        ConnectionValidateResultDetail connectionValidateResultDetail = rocksDBTestConnection.handleFileRead(toLongPathName);
        assertEquals("failed", connectionValidateResultDetail.getStatus());
    }

}
