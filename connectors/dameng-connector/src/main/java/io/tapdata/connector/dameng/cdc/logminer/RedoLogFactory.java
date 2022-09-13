package io.tapdata.connector.dameng.cdc.logminer;

import io.tapdata.connector.dameng.DamengContext;
import io.tapdata.connector.dameng.cdc.logminer.bean.DamengInstanceInfo;
import io.tapdata.connector.dameng.cdc.logminer.bean.DamengRedoLogBatch;
import io.tapdata.connector.dameng.cdc.logminer.bean.RedoLog;
import io.tapdata.connector.dameng.cdc.logminer.constant.DamengSqlConstant;
import io.tapdata.constant.TapLog;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.kit.EmptyKit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by tapdata on 28/11/2017.
 */
public class RedoLogFactory {

    private final static String TAG = RedoLogFactory.class.getSimpleName();

    private DamengContext damengContext;
    private AtomicBoolean isRunning;
    private List<DamengInstanceInfo> oracleInstanceInfos;

    public RedoLogFactory() {
    }

    public RedoLogFactory(DamengContext damengContext, AtomicBoolean isRunning) {
        this.damengContext = damengContext;
        this.isRunning = isRunning;
    }

    public List<DamengInstanceInfo> getOracleInstanceInfos() {
        return oracleInstanceInfos;
    }

    public void setOracleInstanceInfos(List<DamengInstanceInfo> oracleInstanceInfos) {
        this.oracleInstanceInfos = oracleInstanceInfos;
    }

    public synchronized DamengRedoLogBatch produceRedoLog(Long lastScn,Long nextScn) throws SQLException, InterruptedException {
        Connection connection = damengContext.getConnection();
        PreparedStatement archivedLogStmt = null;
        DamengRedoLogBatch oracleRedoLogBatch;
        try {
            archivedLogStmt = connection.prepareStatement(DamengSqlConstant.ARCHIVED_LOG_SQL);
            oracleRedoLogBatch = batchLoadLogs(archivedLogStmt, lastScn, nextScn, 1);
        } catch (Exception e) {
            TapLogger.error(TAG, TapLog.CONN_ERROR_0001.getMsg(), e.getMessage(), e);
            throw e;
        } finally {
            if (archivedLogStmt != null) {
                archivedLogStmt.close();
            }
        }
        return oracleRedoLogBatch;
    }

    private DamengRedoLogBatch batchLoadLogs(PreparedStatement archivedLogStmt, Long lastScn,Long nextScn, int batchSize) throws SQLException, InterruptedException {
        DamengRedoLogBatch oracleRedoLogBatch = null;
        while (isRunning.get()) {
            List<RedoLog> redoLogList = batchLoadArchivedLogs(archivedLogStmt, lastScn,nextScn, batchSize);
            if (EmptyKit.isNotEmpty(redoLogList)) {
                oracleRedoLogBatch = new DamengRedoLogBatch(redoLogList, false);
                break;
            }
           Thread.sleep(1000);
        }
        return oracleRedoLogBatch;
    }

    private List<RedoLog> batchLoadArchivedLogs(PreparedStatement archivedLogStmt, Long lastScn, Long nextScn, int batchSize) throws SQLException {
        List<RedoLog> redoLogList = new ArrayList<>();
        archivedLogStmt.setLong(1, lastScn);
        archivedLogStmt.setLong(2, nextScn);
        archivedLogStmt.setInt(3, batchSize);
        ResultSet resultSet = archivedLogStmt.executeQuery();
        while (resultSet.next()) {
            RedoLog analysisRedoLog = RedoLog.archivedLog(resultSet);
            redoLogList.add(analysisRedoLog);
        }
        return redoLogList;
    }

    private List<RedoLog> batchLoadOnlineLogs(PreparedStatement onlineLogStmt, Long lastScn, int batchSize) throws SQLException {
        List<RedoLog> redoLogList = new ArrayList<>();
        onlineLogStmt.setLong(1, lastScn);
        onlineLogStmt.setInt(2, batchSize);
        ResultSet resultSet = onlineLogStmt.executeQuery();
        while (resultSet.next()) {
            RedoLog analysisRedoLog = RedoLog.archivedLog(resultSet);
            redoLogList.add(analysisRedoLog);
        }
        return redoLogList;
    }

}
