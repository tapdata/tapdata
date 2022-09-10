package io.tapdata.connector.dameng.cdc.logminer;

import io.tapdata.connector.dameng.DamengContext;
import io.tapdata.connector.dameng.cdc.logminer.bean.OracleInstanceInfo;
import io.tapdata.connector.dameng.cdc.logminer.bean.OracleRedoLogBatch;
import io.tapdata.connector.dameng.cdc.logminer.bean.RedoLog;
import io.tapdata.connector.dameng.cdc.logminer.constant.OracleSqlConstant;
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
    private List<OracleInstanceInfo> oracleInstanceInfos;

    public RedoLogFactory() {
    }

    public RedoLogFactory(DamengContext damengContext, AtomicBoolean isRunning) {
        this.damengContext = damengContext;
        this.isRunning = isRunning;
    }

    public List<OracleInstanceInfo> getOracleInstanceInfos() {
        return oracleInstanceInfos;
    }

    public void setOracleInstanceInfos(List<OracleInstanceInfo> oracleInstanceInfos) {
        this.oracleInstanceInfos = oracleInstanceInfos;
    }

    public synchronized OracleRedoLogBatch produceRedoLog(Long lastScn) throws SQLException, InterruptedException {
        Connection connection = damengContext.getConnection();
        PreparedStatement archivedLogStmt = null;
        OracleRedoLogBatch oracleRedoLogBatch;
        try {
            archivedLogStmt = connection.prepareStatement(OracleSqlConstant.ARCHIVED_LOG_SQL);
            oracleRedoLogBatch = batchLoadLogs(archivedLogStmt, lastScn, 1);
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

    private OracleRedoLogBatch batchLoadLogs(PreparedStatement archivedLogStmt, Long lastScn, int batchSize) throws SQLException, InterruptedException {
        OracleRedoLogBatch oracleRedoLogBatch = null;
        while (isRunning.get()) {
            List<RedoLog> redoLogList = batchLoadArchivedLogs(archivedLogStmt, lastScn, batchSize);
            if (EmptyKit.isNotEmpty(redoLogList)) {
                oracleRedoLogBatch = new OracleRedoLogBatch(redoLogList, false);
                break;
            }
           Thread.sleep(1000);
        }
        return oracleRedoLogBatch;
    }

    private  List<RedoLog> batchLoadArchivedLogs(PreparedStatement archivedLogStmt,  Long lastScn, int batchSize) throws SQLException {
            List<RedoLog> redoLogList = new ArrayList<>();
            archivedLogStmt.setLong(1, lastScn);
            archivedLogStmt.setInt(2, batchSize);
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
