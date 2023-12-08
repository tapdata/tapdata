package io.tapdata.schema;

import lombok.SneakyThrows;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class TapTableMapTest {
    private TapTableMap tapTableMap;
    private Map tableNameAndQualifiedNameMap;
    List<String> tableNames;

    @BeforeEach
    void buildTapTableMap(){
        tapTableMap = mock(TapTableMap.class);
        tableNameAndQualifiedNameMap = new ConcurrentHashMap();
        tableNameAndQualifiedNameMap.put("table1","table1");
        tableNameAndQualifiedNameMap.put("table2","table2");
        tableNameAndQualifiedNameMap.put("table3","table3");
        ReflectionTestUtils.setField(tapTableMap,"tableNameAndQualifiedNameMap",tableNameAndQualifiedNameMap);
        tableNames = new ArrayList<>(tableNameAndQualifiedNameMap.keySet());
    }
    @Nested
    @DisplayName("preload schema method test")
    class PreLoadSchemaTest{
        @Test
        @DisplayName("preload has been finished")
        void test1(){
            doCallRealMethod().when(tapTableMap).preLoadSchema();
            tapTableMap.preLoadSchema();
            verify(tapTableMap).preLoadSchema();
        }
        @Test
        @DisplayName("start thread to preload")
        void test2(){
            AtomicLong allCostTs = new AtomicLong(0L);
            when(tapTableMap.preLoadSchema(anyList(),anyInt(),any())).thenReturn(1);
            doCallRealMethod().when(tapTableMap).preLoadSchema();
            tapTableMap.preLoadSchema();
            verify(tapTableMap).preLoadSchema();
        }
    }
    @Nested
    @DisplayName("preload schema return index method test")
    class PreLoadSchemaIndexTest{
        @Test
        @DisplayName("test preload schema without interceptor")
        void test1(){
            doCallRealMethod().when(tapTableMap).preLoadSchema(tableNames, 0,null);
            int actual = tapTableMap.preLoadSchema(tableNames,0, null);
            assertEquals(3,actual);
        }
        @Test
        @DisplayName("test preload schema with interceptor")
        void test2(){
            Function<Long, Boolean> costInterceptor = mock(Function.class);
            doCallRealMethod().when(tapTableMap).preLoadSchema(tableNames,0, costInterceptor);
            int actual = tapTableMap.preLoadSchema(tableNames,0,costInterceptor);
            assertEquals(3,actual);
        }
        @Test
        @DisplayName("test preload schema intercept thread")
        void test3(){
            Function<Long, Boolean> costInterceptor = new Function<Long, Boolean>() {
                @Override
                public Boolean apply(Long aLong) {
                    return true;
                }
            };
            doCallRealMethod().when(tapTableMap).preLoadSchema(tableNames,0, costInterceptor);
            int actual = tapTableMap.preLoadSchema(tableNames,0,costInterceptor);
            assertEquals(1,actual);
        }
    }
    @Nested
    @DisplayName("doClose method test")
    class DoCloseTest{
        @Test
        @DisplayName("do close with null future")
        void test1(){
            CompletableFuture future = null;
            ReflectionTestUtils.setField(tapTableMap,"future",future);
            doCallRealMethod().when(tapTableMap).doClose();
            tapTableMap.doClose();
            verify(tapTableMap).doClose();
        }
        @Test
        @DisplayName("do close with future")
        void test2(){
            ExecutorService executorService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
            CompletableFuture future = CompletableFuture.runAsync(() -> {
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }, executorService);
            ReflectionTestUtils.setField(tapTableMap,"future",future);
            doCallRealMethod().when(tapTableMap).doClose();
            tapTableMap.doClose();
            assertEquals(true,future.isCancelled());
        }
    }
}
