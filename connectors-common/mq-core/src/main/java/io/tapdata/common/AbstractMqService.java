package io.tapdata.common;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
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
        List<TapTable> tableList = TapSimplify.list();
        for (T topic : destinationSet) {
            TapTable table = new TapTable();
            SCHEMA_PARSER.parse(table, analyzeTable(object, topic, table));
            tableList.add(table);
            if (tableList.size() >= tableSize) {
                consumer.accept(tableList);
                tableList = TapSimplify.list();
            }
        }
        if (EmptyKit.isNotEmpty(tableList)) {
            consumer.accept(tableList);
        }
    }

    protected abstract <T> Map<String, Object> analyzeTable(Object object, T topic, TapTable tapTable) throws Exception;
}
