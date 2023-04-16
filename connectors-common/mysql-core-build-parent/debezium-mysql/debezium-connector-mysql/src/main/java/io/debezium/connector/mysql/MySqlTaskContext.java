/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

import java.lang.reflect.Field;

/**
 * A state (context) associated with a MySQL task
 *
 * @author Jiri Pechanec
 *
 */
public class MySqlTaskContext extends CdcSourceTaskContext {

    private final MySqlDatabaseSchema schema;
    private final BinaryLogClient binaryLogClient;
    private final TopicSelector<TableId> topicSelector;

    public MySqlTaskContext(MySqlConnectorConfig config, MySqlDatabaseSchema schema) {
        super(config.getContextName(), config.getLogicalName(), schema::tableIds);
        this.schema = schema;
        this.binaryLogClient = new BinaryLogClient(config.hostname(), config.port(), config.username(), config.password());
        try {
            //This try for td-sql and set partition id for td-sql's binary log client @Gavin
            Configuration configuration = config.getConfig();
            if(configuration != null) {
                String tdsqlPartition = configuration.getString("tdsql.partition");
                if(tdsqlPartition != null) {
                    tdsqlPartition = tdsqlPartition.trim();
                    Class<? extends BinaryLogClient> clientClass = binaryLogClient.getClass();
                    Field sql = clientClass.getDeclaredField("setPartitionId");
                    sql.setAccessible(true);
                    sql.set(binaryLogClient,  tdsqlPartition);
                    //LOGGER.info("Partition of TD-SQL BinaryLog.");
                }
            }
        } catch(Throwable ignore) {
        }
        topicSelector = MySqlTopicSelector.defaultSelector(config);
    }

    public MySqlDatabaseSchema getSchema() {
        return schema;
    }

    public BinaryLogClient getBinaryLogClient() {
        return binaryLogClient;
    }

    public TopicSelector<TableId> getTopicSelector() {
        return topicSelector;
    }
}
