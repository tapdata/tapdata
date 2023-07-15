package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.dto.read.CdcPosition;
import io.tapdata.sybase.cdc.dto.start.CdcStartVariables;
import io.tapdata.sybase.cdc.dto.start.CommandType;
import io.tapdata.sybase.cdc.dto.start.LivenessMonitor;
import io.tapdata.sybase.cdc.dto.start.SybaseDstLocalStorage;
import io.tapdata.sybase.cdc.dto.start.SybaseFilterConfig;
import io.tapdata.sybase.cdc.dto.start.SybaseGeneralConfig;
import io.tapdata.sybase.cdc.dto.start.SybaseSrcConfig;
import io.tapdata.sybase.cdc.dto.watch.FileMonitor;
import io.tapdata.sybase.cdc.dto.watch.StopLock;
import io.tapdata.sybase.extend.ConnectionConfig;
import io.tapdata.sybase.util.Code;
import org.apache.commons.io.FilenameUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.tapdata.base.ConnectorBase.list;
import static io.tapdata.base.ConnectorBase.map;

/**
 * @author GavinXiao
 * @description StartCdc create by Gavin
 * @create 2023/7/13 11:49
 **/
public class CdcHandle {
    CdcRoot root;
    StopLock lock;
    TapConnectionContext context;
    final FileMonitor fileMonitor = new FileMonitor(1000);

    public CdcHandle(CdcRoot root, TapConnectionContext context, StopLock lock) {
        this.root = root;
        this.lock = lock;
        this.context = context;
        String sybasePocPath = root.getSybasePocPath();

        //@todo set CdcStartVariables from context config
        CdcStartVariables startVariables = CdcStartVariables.create();
        ConnectionConfig connectionConfig = new ConnectionConfig(context);
        List<SybaseFilterConfig> filterConfigs = new ArrayList<>();
        SybaseFilterConfig filterConfig = new SybaseFilterConfig();
        filterConfig.setCatalog(connectionConfig.getDatabase());
        filterConfig.setSchema(connectionConfig.getUsername());
        filterConfig.setTypes(list("TABLE", "VIEW"));

        List<String> cdcTables = root.getCdcTables();
        if (null == cdcTables || cdcTables.isEmpty()) {
            throw new CoreException(Code.STREAM_READ_WARN, "Not any table need to cdc");
        }
        Map<String, Object> tables = map();
        for (String cdcTable : cdcTables) {
            tables.put(cdcTable, null);
        }
        filterConfig.setAllow(tables);


        SybaseSrcConfig srcConfig = new SybaseSrcConfig();
        srcConfig.setType("SYBASE_ASE");
        srcConfig.setDatabase(connectionConfig.getDatabase());
        srcConfig.setHost(connectionConfig.getHost());
        srcConfig.setPort(connectionConfig.getPort());
        srcConfig.setPassword(connectionConfig.getPassword());
        srcConfig.setUsername(connectionConfig.getUsername());
        srcConfig.setMax_connections(2);
        srcConfig.setMax_retries(10);
        srcConfig.setRetry_wait_duration_ms(1000);
        srcConfig.setTransaction_store_location(FilenameUtils.concat(sybasePocPath, "/config/sybase2csv/data"));
        srcConfig.setTransaction_store_cache_limit(100000);


        SybaseDstLocalStorage dstLocalStorage = new SybaseDstLocalStorage();
        dstLocalStorage.setStorage_location(FilenameUtils.concat(sybasePocPath, "/config/sybase2csv/csv"));
        dstLocalStorage.setFile_format("CSV");
        dstLocalStorage.setType("LOCALSTORAGE");

        SybaseGeneralConfig generalConfig = new SybaseGeneralConfig();
        LivenessMonitor monitor = new LivenessMonitor();
        monitor.setEnable(true);
        monitor.setInactive_timeout_ms(900_000);
        monitor.setMin_free_memory_threshold_percent(5);
        monitor.setLiveness_check_interval_ms(60_000);
        generalConfig.setLiveness_monitor(monitor);
        generalConfig.setTrace_dir(FilenameUtils.concat(sybasePocPath, "/config/sybase2csv/trace"));
        generalConfig.setData_dir(FilenameUtils.concat(sybasePocPath, "/config/sybase2csv/data"));
        generalConfig.setError_connection_tracing(FilenameUtils.concat(sybasePocPath, "/config/sybase2csv/trace"));
        generalConfig.setLicense_path(FilenameUtils.concat(sybasePocPath, "/replicant-cli/"));

        this.root.setVariables(
                startVariables
                        .filterConfig(filterConfigs)
                        .srcConfig(srcConfig)
                        .sybaseDstLocalStorage(dstLocalStorage)
                        .sybaseGeneralConfig(generalConfig)
        );
    }

    //Step #1
    public void startCdc() {
        CdcRoot compileBaseFile = new ConfigBaseField(root, "").compile();
        //String pocPath = compileBaseFile.getSybasePocPath();
        CdcRoot compileYaml = new ConfigYaml(
                compileBaseFile,
                root.getVariables().getFilterConfig(),
                root.getVariables().getSrcConfig(),
                root.getVariables().getSybaseDstLocalStorage(),
                root.getVariables().getSybaseGeneralConfig()
        ).compile();
        new ExecCommand(compileYaml, CommandType.FULL).compile();
    }

    //Step #2
    public CdcPosition startListen(
            String monitorPath,
            String monitorFileName,
            List<String> tables,
            CdcPosition position,
            int batchSize,
            StreamReadConsumer consumer) {
        if (null == position) position = new CdcPosition();
        new ListenFile(this.root,
                monitorPath,
                tables,
                monitorFileName,
                new AnalyseCsvFile(this.root, position, null),
                lock,
                batchSize,
                consumer
        ).monitor(fileMonitor).compile();
        return position;
    }

    //Step #end 1
    public void releaseCdc() {
        Optional.ofNullable(root.getProcess()).ifPresent(Process::destroy);
        Optional.ofNullable(fileMonitor).ifPresent(FileMonitor::stop);
    }

    //Step #end 2
    public void stopCdc() {
        Optional.ofNullable(root.getProcess()).ifPresent(Process::destroy);
        Optional.ofNullable(fileMonitor).ifPresent(FileMonitor::stop);
    }

    public CdcRoot getRoot() {
        return root;
    }

    public void setRoot(CdcRoot root) {
        this.root = root;
    }
}
