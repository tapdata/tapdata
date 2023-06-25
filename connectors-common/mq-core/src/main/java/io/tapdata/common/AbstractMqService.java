package io.tapdata.common;

import com.google.common.collect.Lists;
import io.tapdata.constant.MqTestItem;
import io.tapdata.entity.event.ddl.table.TapFieldBaseEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;
import io.tapdata.util.JsonSchemaParser;
import io.tapdata.util.NetUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

public abstract class AbstractMqService implements MqService {

    private static final String TAG = AbstractMqService.class.getSimpleName();
    protected final static long SINGLE_MAX_LOAD_TIMEOUT = TimeUnit.SECONDS.toMillis(2L);
    protected final static JsonSchemaParser SCHEMA_PARSER = new JsonSchemaParser();
    protected final AtomicBoolean consuming = new AtomicBoolean(false);
    protected final static int concurrency = 5;
    protected ExecutorService executorService;
    protected MqConfig mqConfig;
    protected Log tapLogger;

    public void setTapLogger(Log tapLogger) {
        this.tapLogger = tapLogger;
    }

    @Override
    public TestItem testHostAndPort() {
        if (EmptyKit.isBlank(mqConfig.getNameSrvAddr())) {
            try {
                NetUtil.validateHostPortWithSocket(mqConfig.getMqHost(), mqConfig.getMqPort());
                return testItem(MqTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY);
            } catch (IOException e) {
                return testItem(MqTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, e.getMessage());
            }
        } else {
            String[] hostAndPort = mqConfig.getNameSrvAddr().split(",");
            int failedCount = 0;
            for (String hostAndPortItem : hostAndPort) {
                String[] strs = hostAndPortItem.split(":");
                if (strs.length != 2) {
                    return testItem(MqTestItem.NAME_SERVER.getContent(), TestItem.RESULT_FAILED, "name server address is invalid!");
                } else {
                    try {
                        NetUtil.validateHostPortWithSocket(strs[0], Integer.parseInt(strs[1]));
                    } catch (IOException e) {
                        failedCount++;
                    } catch (NumberFormatException e) {
                        return testItem(MqTestItem.NAME_SERVER.getContent(), TestItem.RESULT_FAILED, "name server address is invalid!");
                    }
                }
            }
            if (failedCount == 0) {
                return testItem(MqTestItem.NAME_SERVER.getContent(), TestItem.RESULT_SUCCESSFULLY);
            } else if (failedCount == hostAndPort.length) {
                return testItem(MqTestItem.NAME_SERVER.getContent(), TestItem.RESULT_FAILED, "all addresses of name server is down!");
            } else {
                return testItem(MqTestItem.NAME_SERVER.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "some addresses of name server is down!");
            }
        }
    }

    @Override
    public ConnectionCheckItem testPing() {
        long start = System.currentTimeMillis();
        ConnectionCheckItem connectionCheckItem = ConnectionCheckItem.create();
        connectionCheckItem.item(ConnectionCheckItem.ITEM_PING);
        if (EmptyKit.isBlank(mqConfig.getNameSrvAddr())) {
            try {
                NetUtil.validateHostPortWithSocket(mqConfig.getMqHost(), mqConfig.getMqPort());
                return connectionCheckItem.result(ConnectionCheckItem.RESULT_SUCCESSFULLY)
                        .takes(System.currentTimeMillis() - start);
            } catch (IOException e) {
                return connectionCheckItem.result(ConnectionCheckItem.RESULT_FAILED)
                        .information(e.getMessage())
                        .takes(System.currentTimeMillis() - start);
            }
        } else {
            String[] hostAndPort = mqConfig.getNameSrvAddr().split(",");
            int failedCount = 0;
            for (String hostAndPortItem : hostAndPort) {
                String[] strs = hostAndPortItem.split(":");
                if (strs.length != 2) {
                    return connectionCheckItem.result(ConnectionCheckItem.RESULT_FAILED)
                            .information("name server address is invalid!")
                            .takes(System.currentTimeMillis() - start);
                } else {
                    try {
                        NetUtil.validateHostPortWithSocket(strs[0], Integer.parseInt(strs[1]));
                    } catch (IOException e) {
                        failedCount++;
                    } catch (NumberFormatException e) {
                        return connectionCheckItem.result(ConnectionCheckItem.RESULT_FAILED)
                                .information("name server address is invalid!")
                                .takes(System.currentTimeMillis() - start);
                    }
                }
            }
            if (failedCount == 0) {
                connectionCheckItem.result(ConnectionCheckItem.RESULT_SUCCESSFULLY);
            } else if (failedCount == hostAndPort.length) {
                connectionCheckItem.result(ConnectionCheckItem.RESULT_FAILED).information("all addresses of name server is down!");
            } else {
                connectionCheckItem.result(ConnectionCheckItem.RESULT_SUCCESSFULLY_WITH_WARN).information("some addresses of name server is down!");
            }
            connectionCheckItem.takes(System.currentTimeMillis() - start);
            return connectionCheckItem;
        }
    }

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

    protected <T> void submitTables(int tableSize, Consumer<List<TapTable>> consumer, Object object, Set<T> destinationSet) {
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
            } finally {
                executorService.shutdown();
            }
            consumer.accept(tableList);
        });
    }

    protected abstract <T> Map<String, Object> analyzeTable(Object object, T topic, TapTable tapTable) throws Exception;

    @Override
    public void produce(TapFieldBaseEvent tapFieldBaseEvent) throws Throwable {
        throw new UnsupportedOperationException("DDL unsupported");
    }
}
