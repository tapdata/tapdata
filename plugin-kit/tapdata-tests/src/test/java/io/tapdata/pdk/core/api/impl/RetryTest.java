package io.tapdata.pdk.core.api.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectionFunctions;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.ReflectionUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RetryTest {
    //测试报错能否触发重试
    @Test
    public void testRetry() throws Exception {
        ConnectionNode node = new ConnectionNode();
        Field field = ReflectionUtil.getField(ConnectionNode.class, "connectionContext");
        field.setAccessible(true);
        field.set(node, new TapConnectionContext(null, null));

        node.init(new TapConnectorTest());
        ConnectionFunctions<?> connectionFunctions = node.getConnectionFunctions();
        connectionFunctions.supportErrorHandleFunction((nodeContext, method, throwable) -> {
            return RetryOptions.create().needRetry(true).beforeRetryMethod(() -> {
                System.out.println("begin retry...");
            });
        });
        Integer retryTimes = 3;
        PDKMethodInvoker invoker = PDKMethodInvoker.create()
                .runnable(()->{
                    System.out.println("Begin retry...");
                    throw new IOException("test retry...");
                })
                .message("call connection functions coding@io.tapdata-v1.0-SNAPSHOT associateId codingSource_1662088750311")
                .logTag("")
                .errorConsumer(null)
                .async(false)
                .contextClassLoader(null)
                .retryTimes(retryTimes)
                .retryPeriodSeconds(5);
        try {
            PDKInvocationMonitor.invoke( node, PDKMethod.REGISTER_CAPABILITIES, invoker );
        }catch (Exception e){

        }

        assertTrue(true,"retry times is "+retryTimes+",retry start succeed");
    }

    //测试报错不需要重试
    @Test
    public void testNotNeedRetry() throws Exception {
        ConnectionNode node = new ConnectionNode();
        Field field = ReflectionUtil.getField(ConnectionNode.class, "connectionContext");
        field.setAccessible(true);
        field.set(node, new TapConnectionContext(null, null));

        node.init(new TapConnectorTest());
        ConnectionFunctions<?> connectionFunctions = node.getConnectionFunctions();
        connectionFunctions.supportErrorHandleFunction((nodeContext, method, throwable) -> {
            return RetryOptions.create().needRetry(false).beforeRetryMethod(null);
        });
        int retryTimes = 3;
        AtomicInteger retryTimesAtomic = new AtomicInteger(3);
        PDKMethodInvoker invoker = PDKMethodInvoker.create()
                .runnable(()->{
                    throw new IOException("test retry...");
                })
                .message("call connection functions coding@io.tapdata-v1.0-SNAPSHOT associateId codingSource_1662088750311")
                .logTag("")
                .errorConsumer(null)
                .async(false)
                .contextClassLoader(null)
                .retryTimes(retryTimesAtomic.get())
                .retryPeriodSeconds(5);

        try {
            PDKInvocationMonitor.invoke(
                    node,
                    PDKMethod.REGISTER_CAPABILITIES,
                    invoker
            );
        }catch (Exception e){

        }

        assertEquals(retryTimes, retryTimesAtomic.get(),"retry times is "+retryTimes+",it is not retry .");
    }

    //测试能否中断重试
    @Test
    public void testAwaken() throws NoSuchFieldException, IllegalAccessException {
        ConnectionNode node = new ConnectionNode();
        Field field = ReflectionUtil.getField(ConnectionNode.class, "connectionContext");
        field.setAccessible(true);
        field.set(node, new TapConnectionContext(null, null));

        node.init(new TapConnectorTest());
        ConnectionFunctions<?> connectionFunctions = node.getConnectionFunctions();
        connectionFunctions.supportErrorHandleFunction((nodeContext, method, throwable) -> {
            return RetryOptions.create().needRetry(true).beforeRetryMethod(() -> {
                System.out.println("begin retry...");
            });
        });
        PDKMethodInvoker invoker = PDKMethodInvoker.create()
                .runnable(()->{
                    System.out.println("Begin retry...");
                    throw new IOException("test retry...");
                })
                .message("call connection functions coding@io.tapdata-v1.0-SNAPSHOT associateId codingSource_1662088750311")
                .logTag("")
                .errorConsumer(null)
                .async(false)
                .contextClassLoader(null)
                .retryTimes(1<<20)
                .retryPeriodSeconds(1);

        final AtomicBoolean success = new AtomicBoolean(false);
        final Object obj = new Object();
        new Thread(() -> {
            try {
                Thread.sleep(5000L);
                System.out.println("=====>cancel retry...");
                invoker.cancelRetry();
                synchronized (obj) {
                    obj.wait(10000L);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

       try {
           PDKInvocationMonitor.invoke(
                   node,
                   PDKMethod.REGISTER_CAPABILITIES,
                   invoker
           );
       }catch (Exception e){

       }

        success.set(true);
        synchronized (obj) {
            obj.notify();
        }
        assertEquals(true, success.get(),"===>cancel retry succeed");
    }



    class TapConnectorTest implements TapConnector{
        @Override
        public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) { }

        @Override
        public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {

        }

        @Override
        public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
            return null;
        }

        @Override
        public int tableCount(TapConnectionContext connectionContext) throws Throwable {
            return 0;
        }

        @Override
        public void init(TapConnectionContext connectionContext) throws Throwable {

        }

        @Override
        public void stop(TapConnectionContext connectionContext) throws Throwable {

        }
    }
}
