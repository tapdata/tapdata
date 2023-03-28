package io.tapdata.connector.postgres.cdc;

import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import io.tapdata.common.cdc.CdcRunner;
import io.tapdata.kit.EmptyKit;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.List;

/**
 * Abstract runner for change data capture
 *
 * @author Jarad
 * @date 2022/5/13
 */
public abstract class DebeziumCdcRunner implements CdcRunner {

    protected EmbeddedEngine engine;
    protected String runnerName;

    protected DebeziumCdcRunner() {

    }

    public String getRunnerName() {
        return runnerName;
    }

    /**
     * records caught by cdc can only be consumed in this method
     */
    public void consumeRecords(List<SourceRecord> sourceRecords, DebeziumEngine.RecordCommitter<SourceRecord> committer) {

    }

    /**
     * start cdc sync
     */
    @Override
    public void startCdcRunner() {
        if (EmptyKit.isNotNull(engine)) {
            engine.run();
        }
    }

    public void stopCdcRunner() {
        if (null != engine && engine.isRunning()) {
            engine.stop();
        }
    }

    @Override
    public boolean isRunning() {
        return null != engine && engine.isRunning();
    }

    /**
     * close cdc sync
     */
    @Override
    public void closeCdcRunner() throws IOException {
        engine.close();
    }

    @Override
    public void run() {
        startCdcRunner();
    }

}
