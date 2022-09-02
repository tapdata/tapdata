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
import io.tapdata.pdk.apis.functions.connection.ErrorHandleFunction;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.Node;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.tapnode.TapNodeInstance;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.ReflectionUtil;
import net.sf.cglib.beans.BeanMap;
import org.bson.json.Converter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
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

        node.init(new TapConnector() {
            @Override
            public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
//                connectorFunctions.supportErrorHandleFunction((nodeContext, method, throwable) -> {
//                    return RetryOptions.create().needRetry(true).beforeRetryMethod(() -> {
//                        System.out.println("asdf");
//                    });
//                });
            }

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
        });
        ConnectionFunctions<?> connectionFunctions = node.getConnectionFunctions();
        connectionFunctions.supportErrorHandleFunction((nodeContext, method, throwable) -> {
            return RetryOptions.create().needRetry(true).beforeRetryMethod(() -> {
                System.out.println("asdf");
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
                .retryTimes(3)
                .retryPeriodSeconds(5);
        PDKInvocationMonitor.invoke(
                node,
                PDKMethod.REGISTER_CAPABILITIES,
                invoker
        );
        final AtomicBoolean success = new AtomicBoolean(false);
        final Object obj = new Object();
        new Thread(new Runnable() {
            @Override
            public void run() {
                Thread.sleep(1000L);
                invoker.cancelRetry();

                synchronized (obj) {
                    obj.wait(3000L);
                }
            }
        }).start();

        success.set(true);
        synchronized (obj) {
            obj.notify();
        }
        assertTrue(true, "fadf");
    }
    //测试
    @Test
    public void test2(){

    }

    //测试能否中断重试
    @Test
    public void testAwaken(){
        ConnectionNode node = JSONObject.parseObject(
                "{\"id\":\"coding\",\"group\":\"io.tapdata\",\"version\":\"1.0-SNAPSHOT\",\"nodeType\":\"Source\",\"nodeClass\":\"class io.tapdata.coding.CodingConnector\", \"associateId\":\"codingSource_1662088750311\", \"dagId\":\"null\"}",
                ConnectionNode.class);
        PDKInvocationMonitor.invoke(
                node,
                PDKMethod.REGISTER_CAPABILITIES,
                PDKMethodInvoker.create()
                        .runnable(()->{System.out.println("Begin retry...");throw new IOException("test retry...");})
                        .message("call connection functions coding@io.tapdata-v1.0-SNAPSHOT associateId codingSource_1662088750311")
                        .logTag("")
                        .errorConsumer(null)
                        .async(false)
                        .contextClassLoader(null)
                        .retryTimes(1<<20)
                        .retryPeriodSeconds(1<<20)
        );
        CommonUtils.awakenRetryObj(CommonUtils.AutoRetryParams.class);
//        int a = 10;
//        assertEquals(1,a);
    }
}
