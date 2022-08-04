package io.tapdata.common;

import com.google.common.collect.Lists;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public abstract class AbstractMqService implements MqService {

    private static final String TAG = AbstractMqService.class.getSimpleName();
    protected final static long MAX_LOAD_TIMEOUT = TimeUnit.SECONDS.toMillis(20L);
    protected final static long SINGLE_MAX_LOAD_TIMEOUT = TimeUnit.SECONDS.toMillis(2L);
    protected final static MqSchemaParser SCHEMA_PARSER = new MqSchemaParser();
    protected final AtomicBoolean consuming = new AtomicBoolean(false);
    protected final static int concurrency = 5;
    protected ExecutorService executorService;

    @Override
    public void close() {
        try {
            consuming.set(false);
            executorService.shutdown();
            Thread.sleep(2000);
        } catch (Exception e) {
            TapLogger.error(TAG, "close service error", e);
        }
    }

    protected <T> void submitTables(int tableSize, Consumer<List<TapTable>> consumer, Object object, Set<T> destinationSet) throws Exception {
        Lists.partition(new ArrayList<>(destinationSet), tableSize).forEach(tables -> {
            List<TapTable> tableList = new CopyOnWriteArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(tables.size());
            executorService = Executors.newFixedThreadPool(tables.size());
            tables.forEach(table -> executorService.submit(() -> {
                TapTable tapTable = new TapTable();
                try {
                    SCHEMA_PARSER.parse(tapTable, analyzeTable(object, table, tapTable));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                tableList.add(tapTable);
                countDownLatch.countDown();
            }));
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            executorService.shutdown();
            consumer.accept(tableList);
        });
    }

    protected abstract <T> Map<String, Object> analyzeTable(Object object, T topic, TapTable tapTable) throws Exception;
}
