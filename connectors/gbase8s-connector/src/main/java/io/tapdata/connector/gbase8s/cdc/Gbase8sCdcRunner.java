package io.tapdata.connector.gbase8s.cdc;

import io.tapdata.common.cdc.CdcRunner;
import io.tapdata.common.cdc.ILogMiner;
import io.tapdata.connector.gbase8s.Gbase8sJdbcContext;
import io.tapdata.connector.gbase8s.cdc.logminer.Gbase8sNewLogMiner;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.util.List;

/**
 * Author:Skeet
 * Date: 2023/6/16
 **/
public class Gbase8sCdcRunner implements CdcRunner {
    private final ILogMiner logMiner;

    public Gbase8sCdcRunner(Gbase8sJdbcContext gbase8sJdbcContext, String connectorId, Log logMiner) throws Throwable {
        this.logMiner = new Gbase8sNewLogMiner(gbase8sJdbcContext, connectorId, logMiner);
    }

    public Gbase8sCdcRunner init(List<String> tableList, KVReadOnlyMap<TapTable> tableMap,
                                 Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        logMiner.init(tableList,
                tableMap,
                offsetState,
                recordSize,
                consumer);
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
    public boolean isRunning() throws Throwable {
        return false;
    }

    @Override
    public void run() {

    }
}
