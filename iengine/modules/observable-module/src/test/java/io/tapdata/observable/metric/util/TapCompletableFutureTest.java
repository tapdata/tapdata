package io.tapdata.observable.metric.util;

import com.tapdata.tm.commons.task.dto.TaskDto;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import static org.mockito.Mockito.*;

public class TapCompletableFutureTest {
    TaskDto taskDto;
    @BeforeEach
    void init(){
        taskDto = new TaskDto();
        taskDto.setId(ObjectId.get());
    }

    @Test
    void testTapCompletableFuture() {
        int queueSize = 6;
        TapCompletableFuture tapCompletableFuture = new TapCompletableFuture(queueSize,6000,1000,taskDto);
        Assertions.assertTrue(tapCompletableFuture.getCompletableFuture() != null);
        Map<Integer, List> mapList = (Map<Integer, List>) ReflectionTestUtils.getField(tapCompletableFuture, "mapList");
        Assertions.assertTrue(mapList.size() == 6);
        int indexUse = (int) ReflectionTestUtils.getField(tapCompletableFuture, "indexUse");
        Assertions.assertTrue(indexUse == 0);

    }

    @Test
    void testGetFreeMapList() {
        int queueSize = 1;
        TapCompletableFuture tapCompletableFuture = new TapCompletableFuture(queueSize,6000,1000,taskDto);
        Map<Integer, List> mapList = new HashMap<>();
        List list = new ArrayList();
        list.add(1);
        mapList.put(1,list);
        ReflectionTestUtils.setField(tapCompletableFuture, "mapList",mapList);
        tapCompletableFuture.getFreeMapList();
        int indexUse = (int) ReflectionTestUtils.getField(tapCompletableFuture, "indexUse");
        Assertions.assertTrue(indexUse == -1);
    }


    @Nested
    class TestClearAll {

        @Test
        void testClearTimeout() {
            TapCompletableFuture tapCompletableFuture =new TapCompletableFuture(6,1000,1000,taskDto);
            CompletableFuture completableFutureTmo =tapCompletableFuture.getCompletableFuture().thenRunAsync(()->{
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            tapCompletableFuture.add(completableFutureTmo);
            tapCompletableFuture.clearAll();
            boolean clearAllDataComplete = (boolean) ReflectionTestUtils.getField(tapCompletableFuture, "clearAllDataComplete");
            Assertions.assertFalse(clearAllDataComplete);
        }
        @Test
        void testClearClearAllComplete() {
            TapCompletableFuture tapCompletableFuture =new TapCompletableFuture(6,1000,1000,taskDto);
            CompletableFuture completableFutureTmo =tapCompletableFuture.getCompletableFuture().thenRunAsync(()->{
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            tapCompletableFuture.add(completableFutureTmo);
            tapCompletableFuture.clearAll();
            boolean clearAllDataComplete = (boolean) ReflectionTestUtils.getField(tapCompletableFuture, "clearAllDataComplete");
            boolean pollDataComplete = (boolean) ReflectionTestUtils.getField(tapCompletableFuture, "pollDataComplete");

            Assertions.assertTrue(clearAllDataComplete);
            Assertions.assertTrue(pollDataComplete);
        }

        @Test
        void testClearPollDataComplete() {
            TapCompletableFuture tapCompletableFuture =new TapCompletableFuture(6,1000,1,taskDto);
            ReflectionTestUtils.setField(tapCompletableFuture, "timeout", 1000);
            ReflectionTestUtils.setField(tapCompletableFuture, "maxList", 1);

            CompletableFuture completableFutureTmo =tapCompletableFuture.getCompletableFuture().thenRunAsync(()->{
                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            tapCompletableFuture.add(completableFutureTmo);
            tapCompletableFuture.clearAll();
            boolean pollDataComplete = (boolean) ReflectionTestUtils.getField(tapCompletableFuture, "clearAllDataComplete");
            Assertions.assertFalse(pollDataComplete);
        }

        @Test
        void testClearInterruptedException() throws InterruptedException {
            TapCompletableFuture tapCompletableFuture =new TapCompletableFuture(6,1000,1,taskDto);

            CompletableFuture completableFutureTmo =tapCompletableFuture.getCompletableFuture().thenRunAsync(()->{
                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            tapCompletableFuture.add(completableFutureTmo);
            tapCompletableFuture.clearAll();
            Thread thread = new Thread(() -> tapCompletableFuture.clearAll());
            thread.start();
            thread.interrupt(); // 打断线程
            thread.join(); // 等待线程结束
            boolean pollDataComplete = (boolean) ReflectionTestUtils.getField(tapCompletableFuture, "clearAllDataComplete");
            Assertions.assertFalse(pollDataComplete);
        }
    }

    @Nested
    class TestClearData {

        @Test
        void testClearDataPollEmpty() {
            TapCompletableFuture tapCompletableFuture =mock(TapCompletableFuture.class);
            List<CompletableFuture> list = mock(ArrayList.class);
            tapCompletableFuture.clearData();
            boolean clearAllDataComplete = (boolean) ReflectionTestUtils.getField(tapCompletableFuture, "clearAllDataComplete");
            Assertions.assertFalse(clearAllDataComplete);
            verify(list,times(0)).clear();
        }
        @Test
        void testClearDataPoll() {
            TapCompletableFuture tapCompletableFuture =mock(TapCompletableFuture.class);
            LinkedBlockingQueue<List<CompletableFuture>> completableFutureQueue = new LinkedBlockingQueue(6);
            CompletableFuture completableFutureTmp =CompletableFuture.runAsync(()->{}).thenRunAsync(()->{

            });
            Map<Integer, List> mapList = new HashMap<>();
            ReflectionTestUtils.setField(tapCompletableFuture, "mapList",mapList);
            List<CompletableFuture> list = spy(ArrayList.class);
            list.add(completableFutureTmp);
            completableFutureQueue.offer(list);
            ReflectionTestUtils.setField(tapCompletableFuture, "completableFutureQueue", completableFutureQueue);
            doCallRealMethod().when(tapCompletableFuture).clearData();
            tapCompletableFuture.clearData();
            boolean clearAllDataComplete = (boolean) ReflectionTestUtils.getField(tapCompletableFuture, "clearAllDataComplete");
            Assertions.assertTrue(clearAllDataComplete);
            verify(list,times(1)).clear();
        }

        @Test
        void testClearPollMapList() {
            TapCompletableFuture tapCompletableFuture =mock(TapCompletableFuture.class);
            Map<Integer, List> mapList = new HashMap<>();
            List list = spy(ArrayList.class);
            CompletableFuture completableFutureTmp =CompletableFuture.runAsync(()->{}).thenRunAsync(()->{

            });
            list.add(completableFutureTmp);
            mapList.put(0,list);
            ReflectionTestUtils.setField(tapCompletableFuture, "mapList",mapList);
            doCallRealMethod().when(tapCompletableFuture).clearData();
            ReflectionTestUtils.setField(tapCompletableFuture, "completableFutureQueue", new LinkedBlockingQueue(6));
            ReflectionTestUtils.setField(tapCompletableFuture, "indexUse", 0);

            tapCompletableFuture.clearData();
            boolean clearAllDataComplete = (boolean) ReflectionTestUtils.getField(tapCompletableFuture, "clearAllDataComplete");
            Assertions.assertTrue(clearAllDataComplete);
            verify(list,times(1)).clear();
        }


    }

    @Nested
    class TestAddData {

        @Test
        void testAdd() {
            TapCompletableFuture tapCompletableFuture =new TapCompletableFuture(6,6000,1000,taskDto);
            CompletableFuture completableFutureTmp =CompletableFuture.runAsync(()->{}).thenRunAsync(()->{

            });

            tapCompletableFuture.add(completableFutureTmp);
            Map<Integer, List> mapList = (Map<Integer, List>) ReflectionTestUtils.getField(tapCompletableFuture, "mapList");
            Assertions.assertTrue(mapList.get(0).size()==1);

        }
        @Test
        void testMaxList() {
            TapCompletableFuture tapCompletableFuture =mock(TapCompletableFuture.class);
            LinkedBlockingQueue<List<CompletableFuture>> completableFutureQueue = new LinkedBlockingQueue(6);
            CompletableFuture completableFutureTmp =CompletableFuture.runAsync(()->{}).thenRunAsync(()->{

            });
            Map<Integer, List> mapList = new HashMap<>();
            List list = spy(ArrayList.class);
            mapList.put(0,list);
            ReflectionTestUtils.setField(tapCompletableFuture, "mapList",mapList);
            ReflectionTestUtils.setField(tapCompletableFuture, "maxList", 1);

            ReflectionTestUtils.setField(tapCompletableFuture, "completableFutureQueue", completableFutureQueue);
            doCallRealMethod().when(tapCompletableFuture).add(completableFutureTmp);
            tapCompletableFuture.add(completableFutureTmp);
            Map<Integer, List> actualData = (Map<Integer, List>) ReflectionTestUtils.getField(tapCompletableFuture, "mapList");
            Assertions.assertTrue(actualData.get(0).size()==1);


        }

        @Test
        void testCompletableFutureQueueNotOffer() throws InterruptedException {
            TapCompletableFuture tapCompletableFuture =mock(TapCompletableFuture.class);
            Map<Integer, List> mapList = new HashMap<>();
            List list = spy(ArrayList.class);
            CompletableFuture completableFutureTmp =CompletableFuture.runAsync(()->{}).thenRunAsync(()->{

            });
            list.add(completableFutureTmp);
            mapList.put(0,list);
            ReflectionTestUtils.setField(tapCompletableFuture, "maxList", 1);
            ReflectionTestUtils.setField(tapCompletableFuture, "mapList",mapList);
            doCallRealMethod().when(tapCompletableFuture).clearData();
            LinkedBlockingQueue<List<CompletableFuture>> completableFutureQueue = new LinkedBlockingQueue<>(1);
            completableFutureQueue.offer(list);
            ReflectionTestUtils.setField(tapCompletableFuture, "completableFutureQueue", completableFutureQueue);
            ReflectionTestUtils.setField(tapCompletableFuture, "indexUse", 0);

            doCallRealMethod().when(tapCompletableFuture).add(completableFutureTmp);
            Thread thread = new Thread(() -> tapCompletableFuture.add(completableFutureTmp));
            thread.start();
            thread.interrupt(); // 打断线程

            Map<Integer, List> actualData = (Map<Integer, List>) ReflectionTestUtils.getField(tapCompletableFuture, "mapList");
            Assertions.assertTrue(actualData.get(0).size()==1);
        }
        @Test
        void testCompletableGetFreeMapListNoUse() throws InterruptedException {
            TapCompletableFuture tapCompletableFuture =mock(TapCompletableFuture.class);
            Map<Integer, List> mapList = new HashMap<>();
            List list = spy(ArrayList.class);
            CompletableFuture completableFutureTmp =CompletableFuture.runAsync(()->{}).thenRunAsync(()->{

            });
            list.add(completableFutureTmp);
            mapList.put(0,list);
            ReflectionTestUtils.setField(tapCompletableFuture, "maxList", 1);
            ReflectionTestUtils.setField(tapCompletableFuture, "mapList",mapList);
            doCallRealMethod().when(tapCompletableFuture).clearData();
            LinkedBlockingQueue<List<CompletableFuture>> completableFutureQueue = new LinkedBlockingQueue<>(2);
            ReflectionTestUtils.setField(tapCompletableFuture, "completableFutureQueue", completableFutureQueue);
            ReflectionTestUtils.setField(tapCompletableFuture, "indexUse", 0);

            doCallRealMethod().when(tapCompletableFuture).add(completableFutureTmp);
            Thread thread = new Thread(() -> tapCompletableFuture.add(completableFutureTmp));
            when(tapCompletableFuture.getFreeMapList()).thenReturn(-1);
            thread.start();
            Thread.sleep(3000L);
            Map<Integer, List> actualData = (Map<Integer, List>) ReflectionTestUtils.getField(tapCompletableFuture, "mapList");
            Assertions.assertTrue(actualData.get(0).size()==1);
        }

    }

    @Test
    void testThreadAdd() throws InterruptedException{
        long startTime = System.currentTimeMillis();
        TapCompletableFuture tapCompletableFuture = new TapCompletableFuture(5,6000,10000,taskDto);
        Thread thread1 = new Thread(() -> {
            for (int index =0;index<10000;index++){
                CompletableFuture completableFutureTmp =tapCompletableFuture.getCompletableFuture().thenRunAsync(()->{
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            tapCompletableFuture.add(completableFutureTmp);
            }
        });

        Thread thread2 = new Thread(() -> {
            for (int index =0;index<10000;index++){
                CompletableFuture completableFutureTmp =tapCompletableFuture.getCompletableFuture().thenRunAsync(()->{
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
                tapCompletableFuture.add(completableFutureTmp);
            }
        });

        Thread thread3 = new Thread(() -> {
            for (int index =0;index<10000;index++){
                CompletableFuture completableFutureTmp =tapCompletableFuture.getCompletableFuture().thenRunAsync(()->{
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
                tapCompletableFuture.add(completableFutureTmp);
            }
        });

        thread1.join();
        thread2.join();
        thread3.join();
        tapCompletableFuture.clearAll();
        System.out.println(System.currentTimeMillis()-startTime);
        boolean clearAllDataComplete = (boolean) ReflectionTestUtils.getField(tapCompletableFuture, "clearAllDataComplete");
        boolean pollDataComplete = (boolean) ReflectionTestUtils.getField(tapCompletableFuture, "pollDataComplete");

        Assertions.assertTrue(clearAllDataComplete);
        Assertions.assertTrue(pollDataComplete);
    }

}
