package io.tapdata.connector.dameng.cdc;

import io.tapdata.common.cdc.CdcRunner;
import io.tapdata.common.cdc.ILogMiner;
import io.tapdata.connector.dameng.DamengContext;
import io.tapdata.connector.dameng.cdc.logminer.ManuRedoDamengLogMiner;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.util.List;

public class DamengCdcRunner implements CdcRunner {

    private ILogMiner logMiner;

    public DamengCdcRunner(DamengContext damengContext, String connectorId) throws Throwable {
        logMiner = new ManuRedoDamengLogMiner(damengContext,connectorId);

    }

    public DamengCdcRunner init(List<String> tableList, KVReadOnlyMap<TapTable> tableMap,
                                Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        logMiner.init(
                tableList,
                tableMap,
                offsetState,
                recordSize,
                consumer
        );
        return this;
    }

    @Override
    public void startCdcRunner() throws Throwable {
        logMiner.startMiner();
    }

    @Override
    public void closeCdcRunner() throws Throwable {
        logMiner.stopMiner();
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public void run() {
        try {
            startCdcRunner();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
