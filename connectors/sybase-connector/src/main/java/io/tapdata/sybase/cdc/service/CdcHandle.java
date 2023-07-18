package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.dto.read.CdcPosition;
import io.tapdata.sybase.cdc.dto.start.CdcStartVariables;
import io.tapdata.sybase.cdc.dto.start.CommandType;
import io.tapdata.sybase.cdc.dto.start.LivenessMonitor;
import io.tapdata.sybase.cdc.dto.start.OverwriteType;
import io.tapdata.sybase.cdc.dto.start.SybaseDstLocalStorage;
import io.tapdata.sybase.cdc.dto.start.SybaseExtConfig;
import io.tapdata.sybase.cdc.dto.start.SybaseFilterConfig;
import io.tapdata.sybase.cdc.dto.start.SybaseGeneralConfig;
import io.tapdata.sybase.cdc.dto.start.SybaseSrcConfig;
import io.tapdata.sybase.cdc.dto.watch.FileMonitor;
import io.tapdata.sybase.cdc.dto.watch.StopLock;
import io.tapdata.sybase.extend.ConnectionConfig;
import io.tapdata.sybase.extend.NodeConfig;
import io.tapdata.sybase.util.Code;
import io.tapdata.sybase.util.Utils;
import org.apache.commons.io.FileUtils;

import java.io.File;
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
    TapConnectorContext context;
    FileMonitor fileMonitor;
    final Object closeLock = new Object();

    public CdcHandle(CdcRoot root, TapConnectorContext context, StopLock lock) {
        this.root = root;
        this.lock = lock;
        this.context = context;
        this.root.setContext(context);
    }

    public CdcHandle streamReadConsumer(StreamReadConsumer cdcConsumer, Log log, String monitorPath) {
        this.fileMonitor = new FileMonitor(cdcConsumer, 1000, log, monitorPath);
        return this;
    }

    //Step #1
    public synchronized void startCdc(OverwriteType overwriteType) {
        ConfigBaseField baseField = new ConfigBaseField(root, "");
        if (!baseField.checkStep()) {
            this.root = baseField.compile();
        }
        //String pocPath = compileBaseFile.getSybasePocPath();
        compileYamlConfig();
        CdcStartVariables variables = root.getVariables();
        CdcRoot compileYaml = new ConfigYaml(this.root, variables).compile();
        new ExecCommand(compileYaml, CommandType.CDC, overwriteType).compile();
    }

    private synchronized void compileYamlConfig() {
        String sybasePocPath = root.getSybasePocPath();

        //@todo set CdcStartVariables from context config
        CdcStartVariables startVariables = CdcStartVariables.create();
        ConnectionConfig connectionConfig = new ConnectionConfig(context);


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
        srcConfig.setTransaction_store_location("" + sybasePocPath + "/config/sybase2csv/data");
        srcConfig.setTransaction_store_cache_limit(100000);


        SybaseDstLocalStorage dstLocalStorage = new SybaseDstLocalStorage();
        dstLocalStorage.setStorage_location("" + sybasePocPath + "/config/sybase2csv/csv");
        dstLocalStorage.setFile_format("CSV");
        dstLocalStorage.setType("LOCALSTORAGE");

        SybaseGeneralConfig generalConfig = new SybaseGeneralConfig();
        LivenessMonitor monitor = new LivenessMonitor();
        monitor.setEnable(true);
        monitor.setInactive_timeout_ms(900_000);
        monitor.setMin_free_memory_threshold_percent(5);
        monitor.setLiveness_check_interval_ms(60_000);
        generalConfig.setLiveness_monitor(monitor);
        generalConfig.setTrace_dir("" + sybasePocPath + "/config/sybase2csv/trace");
        generalConfig.setData_dir("" + sybasePocPath + "/config/sybase2csv/data");
        //generalConfig.setError_connection_tracing("" + sybasePocPath + "/config/sybase2csv/trace");
        generalConfig.setLicense_path(root.getCliPath() + "/");
        generalConfig.setError_trace_dir("" + sybasePocPath + "/config/sybase2csv/trace");

        NodeConfig nodeConfig = new NodeConfig(context);
        SybaseExtConfig extConfig = new SybaseExtConfig();
        SybaseExtConfig.Realtime realtime = extConfig.getRealtime();
        realtime.setFetchIntervals(nodeConfig.getFetchInterval());

        this.root.setVariables(
                startVariables
                        .extConfig(extConfig)
                        .filterConfig(compileFilterTableYamlConfig(connectionConfig))
                        .srcConfig(srcConfig)
                        .sybaseDstLocalStorage(dstLocalStorage)
                        .sybaseGeneralConfig(generalConfig)
        );
    }

    public synchronized List<SybaseFilterConfig> compileFilterTableYamlConfig(ConnectionConfig connectionConfig) {
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
        filterConfigs.add(filterConfig);
        CdcStartVariables variables = this.root.getVariables();
        if (null != variables) {
            variables.filterConfig(filterConfigs);
        }
        return filterConfigs;
    }

    public synchronized void initCdc(OverwriteType overwriteType) {
        CdcRoot compileBaseFile = new ConfigBaseField(root, "").compile();
        //String pocPath = compileBaseFile.getSybasePocPath();
        compileYamlConfig();
        CdcRoot compileYaml = new ConfigYaml(compileBaseFile, root.getVariables()).compile();
        new ExecCommand(compileYaml, CommandType.CDC, overwriteType).compile();
    }

    public synchronized void refreshCdc(OverwriteType overwriteType) {
        stopCdc();
        startCdc(overwriteType);
    }

    //Step #2
    public synchronized CdcPosition startListen(
            String monitorPath,
            String monitorFileName,
            List<String> tables,
            CdcPosition position,
            int batchSize,
            StreamReadConsumer consumer) {
        if (null == position) position = new CdcPosition();
        streamReadConsumer(consumer, context.getLog(), monitorPath);
        new ListenFile(this.root,
                monitorPath,
                tables,
                monitorFileName,
                new AnalyseCsvFile(this.root, position, null),
                lock,
                batchSize
        ).monitor(fileMonitor).compile();
        return position;
    }

    //Step #end 1
    public synchronized void releaseCdc() {
        Optional.ofNullable(root.getProcess()).ifPresent(Process::destroy);
        Optional.ofNullable(fileMonitor).ifPresent(FileMonitor::stop);
        KVMap<Object> stateMap = context.getStateMap();
        Object cdcPath = stateMap.get("cdcPath");
        try {
            if (context != null) {
                if (null == cdcPath || "/*".equals(cdcPath) || "".equals(cdcPath.toString().trim())) {
                    return;
                }
                File file = new File(String.valueOf(cdcPath));
                if ("linux".equalsIgnoreCase(System.getProperty("os.name"))) {
                    final String shell = "rm -rf " + cdcPath;
                    root.getContext().getLog().info("clean cdc path: {}", shell);
                    root.getContext().getLog().info(Utils.run(shell));
                    if (file.exists()) {
                        FileUtils.delete(file);
                    }
                } else {
                    FileUtils.delete(file);
                }
            }
        } catch (Exception e) {
            context.getLog().warn("Can not release cdc path, please go to path: {}, and clean the file", cdcPath);
        }
        //Optional.ofNullable()
    }

    //Step #end 2
    public synchronized void stopCdc() {
        //@todo
        Optional.ofNullable(root.getProcess()).ifPresent(Process::destroy);

        //@todo
        root.setProcess(null);
        Optional.ofNullable(fileMonitor).ifPresent(FileMonitor::stop);

        NodeConfig nodeConfig = new NodeConfig(context);
        try {
            //缓冲作用，延时停止，等待数据库进程释放
            closeLock.wait(nodeConfig.getCloseDelayMill());
        } catch (Exception e) {

        }
    }

    public CdcRoot getRoot() {
        return root;
    }

    public void setRoot(CdcRoot root) {
        this.root = root;
    }
}
