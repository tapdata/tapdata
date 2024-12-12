package io.tapdata.proxy.client;

import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.net.data.FileMeta;
import io.tapdata.modules.api.net.data.IncomingData;
import io.tapdata.modules.api.net.data.OutgoingData;
import io.tapdata.modules.api.net.data.Result;
import io.tapdata.modules.api.pdk.PDKUtils;
import io.tapdata.modules.api.proxy.data.ServiceCallerReceived;
import io.tapdata.modules.api.service.SkeletonService;
import io.tapdata.pdk.apis.entity.message.ServiceCaller;
import io.tapdata.wsclient.modules.imclient.IMClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/12/10 10:49
 */
public class ProxySubscriptionManagerTest {
    @Test
    void testHandlerFileMetaForServiceCaller() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException {
        ProxySubscriptionManager proxySubscriptionManager = new ProxySubscriptionManager();

        Field skeletonServiceField = ProxySubscriptionManager.class.getDeclaredField("skeletonService");
        skeletonServiceField.setAccessible(true);
        SkeletonService skeletonService = mock(SkeletonService.class);
        skeletonServiceField.set(proxySubscriptionManager, skeletonService);

        Field imClientField = ProxySubscriptionManager.class.getDeclaredField("imClient");
        imClientField.setAccessible(true);
        IMClient imClient = mock(IMClient.class);
        imClientField.set(proxySubscriptionManager, imClient);

        Object[] args = new Object[]{"test.log"};
        when(skeletonService.call(eq("LogFileService"), eq("downloadFile"), eq(args)))
                .thenReturn(CompletableFuture.supplyAsync(() -> {

                    return FileMeta.builder().build();
                }));
        when(skeletonService.call(eq("test"), eq("test"), eq(args)))
                .thenReturn(CompletableFuture.supplyAsync(() -> {
                    return new Result();
                }));

        OutgoingData outgoingData = new OutgoingData();
        ServiceCallerReceived serviceCallerReceived = new ServiceCallerReceived();
        serviceCallerReceived.setServiceCaller(
                ServiceCaller.create("test")
                        .className("LogFileService")
                        .method("downloadFile")
                        .args(args)
        );
        outgoingData.setMessage(serviceCallerReceived);

        AtomicBoolean expectedHasFileMeta = new AtomicBoolean(true);
        AtomicInteger counter = new AtomicInteger(0);
        when(imClient.sendData(any())).thenAnswer(answer -> {
            Object object = answer.getArgument(0);
            Assertions.assertNotNull(object);

            Assertions.assertInstanceOf(IncomingData.class, object);
            if (expectedHasFileMeta.get())
                Assertions.assertNotNull(((IncomingData)object).getFileMeta());
            else
                Assertions.assertNull(((IncomingData)object).getFileMeta());

            counter.incrementAndGet();

            return CompletableFuture.supplyAsync(() -> null);
        });

        expectedHasFileMeta.set(true);
        try (MockedStatic<InstanceFactory> mockInstanceFactory = mockStatic(InstanceFactory.class);) {
            mockInstanceFactory.when(() -> InstanceFactory.instance(eq(PDKUtils.class))).thenReturn(mock(PDKUtils.class));

            Method handleServiceCallerReceivedMethod = ProxySubscriptionManager.class
                    .getDeclaredMethod("handleServiceCallerReceived", String.class, OutgoingData.class);
            handleServiceCallerReceivedMethod.setAccessible(true);
            handleServiceCallerReceivedMethod.invoke(proxySubscriptionManager, null, outgoingData);

            Assertions.assertEquals(1, counter.get());
        }


        try (MockedStatic<InstanceFactory> mockInstanceFactory = mockStatic(InstanceFactory.class);) {
            mockInstanceFactory.when(() -> InstanceFactory.instance(eq(PDKUtils.class))).thenReturn(mock(PDKUtils.class));

            counter.set(0);
            expectedHasFileMeta.set(false);

            serviceCallerReceived.setServiceCaller(
                    ServiceCaller.create("tset").className("test").method("test").args(args));

            Method handleServiceCallerReceivedMethod = ProxySubscriptionManager.class
                    .getDeclaredMethod("handleServiceCallerReceived", String.class, OutgoingData.class);
            handleServiceCallerReceivedMethod.setAccessible(true);
            handleServiceCallerReceivedMethod.invoke(proxySubscriptionManager, null, outgoingData);

            Assertions.assertEquals(1, counter.get());
        }

    }
}
