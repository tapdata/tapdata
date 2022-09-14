package io.tapdata.connector.dameng.cdc.logminer;

import io.tapdata.connector.dameng.DamengConfig;
import io.tapdata.connector.dameng.DamengContext;
import io.tapdata.connector.dameng.cdc.logminer.bean.DamengInstanceInfo;
import io.tapdata.connector.dameng.cdc.logminer.bean.DamengRedoLogBatch;
import io.tapdata.connector.dameng.cdc.logminer.bean.RedoLog;
import io.tapdata.connector.dameng.cdc.logminer.bean.RedoLogAnalysisBatch;
import io.tapdata.connector.dameng.cdc.logminer.util.JdbcUtil;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import net.openhft.chronicle.queue.ChronicleQueue;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.connector.dameng.cdc.logminer.constant.DamengSqlConstant.*;

public class ManuRedoDamengLogMiner extends DamengLogMiner {

    private final static String TAG = ManuRedoDamengLogMiner.class.getSimpleName();
    private final int redoLogConcurrency;
    private final LinkedBlockingQueue<DamengRedoLogBatch> redoLogBundleForAnalysisQueue;
    private final LinkedBlockingQueue<AnalysisRedoLogBundle> redoLogBundlesForConsume;
    private final ConcurrentHashMap<String, String> cacheRedoLogContent = new ConcurrentHashMap<>();
    private final List<DamengInstanceInfo> oracleInstanceInfos;
    private RedoLogFactory redoLogFactory;
    private boolean isRac;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService executorService;
    private final String rootDirectory;

    protected  int first=0 ;

    long lastScn =0L;

    public Long fristLsn;

    private Long nextScn ;


    public ManuRedoDamengLogMiner(DamengContext damengContext, String connectorId) throws Throwable {
        super(damengContext);
        redoLogConcurrency = damengConfig.getConcurrency();
        oracleInstanceInfos = getInstanceInfos();
        redoLogBundleForAnalysisQueue = new LinkedBlockingQueue<>(redoLogConcurrency * 2);
        redoLogBundlesForConsume = new LinkedBlockingQueue<>(redoLogConcurrency * 2);
        scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
        executorService = Executors.newFixedThreadPool(redoLogConcurrency);
        rootDirectory = ((DamengConfig) damengContext.getConfig()).getWorkDir() + File.separator + connectorId;
        Files.deleteIfExists(Paths.get(rootDirectory));
    }

    @Override
    public void init(List<String> tableList, KVReadOnlyMap<TapTable> tableMap, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        super.init(tableList, tableMap, offsetState, recordSize, consumer);
        isRunning.set(true);
        initRedoLogQueueAndThread();
        redoLogFactory = new RedoLogFactory(damengContext, isRunning);
        redoLogFactory.setOracleInstanceInfos(oracleInstanceInfos);
        Long lastScn = damengOffset.getPendingScn() > 0 && damengOffset.getPendingScn() < damengOffset.getLastScn() ? damengOffset.getPendingScn() : damengOffset.getLastScn();

        AtomicReference<Long> fristLsnTem = new AtomicReference<>(0L);

        damengContext.query(String.format(GET_LAST_PROCESS_ARCHIVED_REDO_LOG_FILE_SQL, lastScn, lastScn), archivedResultSet -> {
            if (archivedResultSet.next()) {
                fristLsnTem.set(archivedResultSet.getLong(1));
            }

        });
        fristLsn =fristLsnTem.get();
        nextScn = fristLsn;
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            int count = 0;
            while (isRunning.get()) {
                count++;
                try {
                    if (count % 10 == 0) {
                        break;
                    }
                    final DamengRedoLogBatch oracleRedoLogBatch = this.redoLogFactory.produceRedoLog(fristLsn,nextScn);
                    if (oracleRedoLogBatch != null) {
                        final List<RedoLog> redoLogList = oracleRedoLogBatch.getRedoLogList();
                        while (isRunning.get() && !redoLogBundleForAnalysisQueue.offer(oracleRedoLogBatch, 20, TimeUnit.SECONDS)) {
                            // nothing to do
                            TapLogger.info(TAG, "Redo log queue for analysis is full, redo log {} waiting to enqueue", oracleRedoLogBatch);
                        }

                       nextScn = redoLogList.get(0).getNextChangeScn();

                    }
                } catch (Throwable e) {
                    TapLogger.warn(TAG, "Get redo log failed {}, error stack {}.", e.getMessage());
                }
            }
        }, 1, 5, TimeUnit.SECONDS);
    }

    @Override
    public void startMiner() throws Throwable {
        setSession();
            try {
                while (isRunning.get()) {
                    final DamengRedoLogBatch oracleRedoLogBatch;
                    try {
                        oracleRedoLogBatch = redoLogBundleForAnalysisQueue.poll(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        break;
                    }
                    if (oracleRedoLogBatch != null) {
                        TapLogger.info(TAG, "Starting analysis redo log {}");

                        doMine(oracleRedoLogBatch);

                    }
                }
            } catch (Exception ignored) {
            }
        }


    protected void doMine(DamengRedoLogBatch oracleRedoLogBatch) throws Exception {
       Long offsetScn = damengOffset.getPendingScn() > 0 && damengOffset.getPendingScn() < damengOffset.getLastScn() ? damengOffset.getPendingScn() : damengOffset.getLastScn();
       lastScn = lastScn>offsetScn?lastScn:offsetScn;
       // Long lastScn =122240L;
        try {
            List<RedoLog> redoLogList = oracleRedoLogBatch.getRedoLogList();

            List<RedoLogAnalysisBatch> redoLogAnalysisBatches;
            redoLogAnalysisBatches = splitArcRedoLogToMultiBatch(redoLogList);
            Set<String> addedRedoLogNames = new HashSet<>();
            if (EmptyKit.isNotEmpty(redoLogAnalysisBatches)) {
                Collections.sort(redoLogAnalysisBatches);
                String addLogminorSql = createAddLogminorSql(redoLogAnalysisBatches, addedRedoLogNames);
                if (StringUtils.isNotEmpty(addLogminorSql)) {
                    try {
                        statement.execute(END_LOG_MINOR_SQL);
                    }catch (Exception e){
                    }
                    statement.execute(addLogminorSql);
                }

                if (isRunning.get()) {
                    try {
                        statement.execute(String.format(START_LOG_MINOR_CONTINUOUS_MINER_SQL, lastScn));
                    } catch (SQLException e) {
                        throw e;
                    }

                    //get redo log analysis result
                    long startSCN = lastScn;
                    resultSet = statement.executeQuery(analyzeLogSql(startSCN));
                    while (resultSet.next() && isRunning.get()) {
                        while (ddlStop.get()) {
                            TapSimplify.sleep(1000);
                        }
                        ResultSetMetaData metaData = resultSet.getMetaData();
                        Map<String, Object> logData = JdbcUtil.buildLogData(
                                metaData,
                                resultSet,
                                damengConfig.getSysZoneId()
                        );
                        logData.put("ROLLBACK",logData.get("ROLL_BACK"));
                        logData.put("XID",bytesToHexString((byte[])logData.get("XID")));
                        logData.put("STATUS",0);
                        analyzeLog(logData);
                        if (logData.get("SCN") != null && logData.get("SCN") instanceof Long) {
                            lastScn = ((Long) logData.get("SCN"));
                        }
                    }
                 }

            }

        } catch (SQLRecoverableException se) {
            throw se;
        } catch (InterruptedException e) {
            // abort it
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public String createAddLogminorSql(List<RedoLogAnalysisBatch> redoLogAnalysisBatches, Set<String> addedRedoLogNames) {
        StringBuilder sb = new StringBuilder();
        if (EmptyKit.isNotEmpty(redoLogAnalysisBatches)) {
            for (RedoLogAnalysisBatch redoLogAnalysisBatch : redoLogAnalysisBatches) {
                List<RedoLog> logs = redoLogAnalysisBatch.getRedoLogs();
                if (!EmptyKit.isEmpty(logs)) {
                    for (RedoLog log : logs) {
                        String name = log.getName();
                        if (!addedRedoLogNames.contains(name)) {
                            sb.append(" SYS.dbms_logmnr.add_logfile(logfilename=>'").append(name).append("',options=>SYS.dbms_logmnr.ADDFILE)");
                            addedRedoLogNames.add(name);
                        }
                    }
                }
            }

        }
        return sb.toString();
    }

    private List<RedoLogAnalysisBatch> splitArcRedoLogToMultiBatch(List<RedoLog> redoLogList) {
        List<RedoLogAnalysisBatch> redoLogAnalysisBatches = new ArrayList<>();
                if (EmptyKit.isNotEmpty(redoLogList)) {
                    for (RedoLog redoLog : redoLogList) {
                        long firstChangeScn = redoLog.getFirstChangeScn();
                        RedoLogAnalysisBatch analysisBatch = new RedoLogAnalysisBatch();
                        analysisBatch.setStartSCN(firstChangeScn);
                        analysisBatch.getRedoLogs().add(redoLog);
                        redoLogAnalysisBatches.add(analysisBatch);
                    }
                }

        return redoLogAnalysisBatches;
    }




    private List<DamengInstanceInfo> getInstanceInfos() throws Throwable {
        List<DamengInstanceInfo> oracleInstanceInfos = new ArrayList<>();
        damengContext.query(GET_INSTANCES_INFO_SQL, resultSet -> {
            while (resultSet.next()) {
                oracleInstanceInfos.add(new DamengInstanceInfo(resultSet));
            }
        });
        return oracleInstanceInfos;
    }




    private static String bytesToHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length);

        for (int index = 0; index < bytes.length; ++index) {
            byte aByte = bytes[index];
            String temp = Integer.toHexString(255 & aByte);
            if (temp.length() < 2) {
                sb.append(0);
            }

            sb.append(temp);
        }

        return sb.toString();

    }

    @Override
    public void stopMiner() throws Throwable {
        scheduledExecutorService.shutdown();
        executorService.shutdown();
        super.stopMiner();
    }

    static class AnalysisRedoLogBundle {

        private DamengRedoLogBatch oracleRedoLogBatch;

        private final ChronicleQueue redoLogContentQueue;

        public AnalysisRedoLogBundle(DamengRedoLogBatch oracleRedoLogBatch, ChronicleQueue redoLogContentQueue) {
            this.oracleRedoLogBatch = oracleRedoLogBatch;
            this.redoLogContentQueue = redoLogContentQueue;
        }

        public DamengRedoLogBatch getOracleRedoLogBatch() {
            return oracleRedoLogBatch;
        }

        public void setOracleRedoLogBatch(DamengRedoLogBatch oracleRedoLogBatch) {
            this.oracleRedoLogBatch = oracleRedoLogBatch;
        }

        public ChronicleQueue getRedoLogContentQueue() {
            return redoLogContentQueue;
        }
    }
}
