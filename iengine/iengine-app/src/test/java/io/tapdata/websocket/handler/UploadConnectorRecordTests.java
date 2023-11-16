package io.tapdata.websocket.handler;

import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import io.tapdata.websocket.WebSocketEventResult;
import io.tapdata.websocket.handler.DownLoadConnectorHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class UploadConnectorRecordTests {

    private DownLoadConnectorHandler downLoadConnectorHandler;

    private Method privateMethod;

    @Before
    public void setUp() throws NoSuchMethodException {
        // 获取类的Class对象
        Class<?> myClass = DownLoadConnectorHandler.class;
        // 获取私有方法的名称
        String methodName = "uploadConnectorRecord";
        // 使用getDeclaredMethod()来获取私有方法
        privateMethod = myClass.getDeclaredMethod(methodName, String.class, ConnectorRecordDto.statusEnum.class,String.class, Consumer.class);
        // 设置私有方法可访问
        privateMethod.setAccessible(true);
        downLoadConnectorHandler = new DownLoadConnectorHandler();
    }

    @Test
    public void testUploadConnectorRecord_FinishStatusEnum() throws InvocationTargetException, IllegalAccessException {
        String mockPdkHash = "a5af410b12afca476edf4a650c133ddf135bf76542a67787ed6f7f7d53ba712";
        String mockMessage = "testMessage";
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("status",ConnectorRecordDto.statusEnum.FINISH.getStatus());
        Consumer<WebSocketEventResult> consumer = s ->{
            Assert.assertEquals(WebSocketEventResult.Type.PROGRESS_REPORTING.getType(),s.getType());
            Assert.assertEquals("SUCCESS",s.getStatus());
            Assert.assertEquals(resultMap, s.getResult());
            Assert.assertNull(s.getError());
        };
        privateMethod.invoke(downLoadConnectorHandler,mockPdkHash,ConnectorRecordDto.statusEnum.FINISH,mockMessage,consumer);
    }

    @Test
    public void testUploadConnectorRecord_FailStatusEnum() throws InvocationTargetException, IllegalAccessException {
        String mockPdkHash = "a5af410b12afca476edf4a650c133ddf135bf76542a67787ed6f7f7d53ba712";
        String mockMessage = "testMessage";
        Consumer<WebSocketEventResult> consumer = s ->{
            System.out.println(s);
            Assert.assertEquals(WebSocketEventResult.Type.PROGRESS_REPORTING.getType(),s.getType());
            Assert.assertEquals("ERROR",s.getStatus());
            Assert.assertEquals(mockMessage, s.getError());
            Assert.assertNull(s.getResult());
        };
        privateMethod.invoke(downLoadConnectorHandler,mockPdkHash,ConnectorRecordDto.statusEnum.FAIL,mockMessage,consumer);
    }

    @Test(expected = InvocationTargetException.class)
    public void testUploadConnectorRecord_Consumer() throws InvocationTargetException, IllegalAccessException {
        String mockPdkHash = null;
        String mockMessage = "testMessage";
        Consumer<WebSocketEventResult> consumer = null;
        privateMethod.invoke(downLoadConnectorHandler,mockPdkHash,ConnectorRecordDto.statusEnum.FAIL,mockMessage,consumer);
    }

    @Test(expected = InvocationTargetException.class)
    public void testUploadConnectorRecord_StatusEnum() throws InvocationTargetException, IllegalAccessException {
        String mockPdkHash = null;
        String mockMessage = "testMessage";
        Consumer<WebSocketEventResult> consumer = s ->{
            Assert.assertEquals(WebSocketEventResult.Type.PROGRESS_REPORTING.getType(),s.getType());
            Assert.assertEquals("SUCCESS",s.getStatus());
            Assert.assertNull(s.getError());
        };
        privateMethod.invoke(downLoadConnectorHandler,mockPdkHash,null,mockMessage,consumer);
    }

}
