package io.tapdata.connector.tdengine.subscribe;

import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.tmq.TaosConsumer;
import com.taosdata.jdbc.tmq.TopicPartition;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.connector.tdengine.TDengineJdbcContext;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.kit.EmptyKit;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class TDengineSubscribe {
    private static final String TAG = TDengineSubscribe.class.getSimpleName();
    private static final AtomicBoolean shutdown = new AtomicBoolean(false);

    private TDengineJdbcContext tdengineJdbcContext;

    private List<String> tables;

    private Object offset;

    public TDengineSubscribe(TDengineJdbcContext tdengineJdbcContext, List<String> tables, Object offset) {
        this.tdengineJdbcContext = tdengineJdbcContext;
        this.tables = tables;
        this.offset = offset;
    }

    public void subscribe(BiConsumer<Map<String, Object>, String> biConsumer, ShutdownCallBack shutdownCallBack) {

        try {
            // prepare
            CommonDbConfig config = tdengineJdbcContext.getConfig();
            Connection connection = tdengineJdbcContext.getConnection();
            // create topic
            List<String> topicList = new ArrayList<>();
            try (Statement statement = connection.createStatement()) {
                if (EmptyKit.isNotEmpty(tables)) {
                    for (String tableName : tables) {
                        String topic = String.format("topic_%s", tableName);
                        statement.executeUpdate(String.format("drop topic if exists %s", topic));
                        statement.executeUpdate(String.format("create topic if not exists %s as select * from %s", topic, tableName));
                        topicList.add(topic);
                    }
                } else {
                    String topic = String.format("topic_%s", config.getDatabase());
                    statement.executeUpdate(String.format("drop topic if exists %s", topic));
                    statement.executeUpdate(String.format("create topic if not exists %s as select * from %s", topic, config.getDatabase()));
                    topicList.add(topic);
                }
            }

            // create consumer
            Properties properties = new Properties();
//            properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, "127.0.0.1:6030");
            properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, String.format("%s:%s", config.getHost(), 6030));
            properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, Boolean.TRUE.toString());
            properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, Boolean.TRUE.toString());
            properties.setProperty(TMQConstants.GROUP_ID, "test_group_id");
            properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
            properties.setProperty(TMQConstants.VALUE_DESERIALIZER,
                    "io.tapdata.connector.tdengine.subscribe.TDengineResultDeserializer");

            // poll data
            try (TaosConsumer<Map<String, Object>> taosConsumer = new TaosConsumer<>(properties)) {
                taosConsumer.subscribe(topicList);
                while (!shutdown.get()) {
                    ConsumerRecords<Map<String, Object>> records = taosConsumer.poll(Duration.ofMillis(100));
                    Optional<TopicPartition> topicPartition = getTopicPartition(records);
                    if (topicPartition.isPresent() && EmptyKit.isNotBlank(this.getTableName(topicPartition.get()))) {
                        for (Map<String, Object> record : records) {
                            biConsumer.accept(record, this.getTableName(topicPartition.get()));
                        }
                    }
                    Thread.sleep(1000);
                }
            }
        } catch (SQLException | NoSuchFieldException | IllegalAccessException | InterruptedException e) {
            TapLogger.error(TAG, "Table data sync error: {}", e.getMessage(), e);
        } finally {
            shutdownCallBack.call();
        }
    }

    private String getTableName(TopicPartition topicPartition) {
        String tableName = topicPartition.getTableName();
        if (EmptyKit.isNotEmpty(tableName)) {
            return tableName;
        }
        if (!topicPartition.getTopic().startsWith("topic_")) {
            return null;
        }
        return topicPartition.getTopic().substring(6);
    }

    public Optional<TopicPartition> getTopicPartition(ConsumerRecords<Map<String, Object>> consumerRecords) throws NoSuchFieldException, IllegalAccessException {
        Field field = consumerRecords.getClass().getDeclaredField("records");
        field.setAccessible(Boolean.TRUE);
        Map<TopicPartition, List<Map<String, Object>>> records = (Map<TopicPartition, List<Map<String, Object>>>) field.get(consumerRecords);
        Set<TopicPartition> topicPartitions = records.keySet();
        return topicPartitions.stream().filter(Objects::nonNull).findFirst();
    }

    @FunctionalInterface
    public interface ShutdownCallBack {
        void call();
    }

}
