package io.tapdata.common;

import com.mongodb.MongoClient;
import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.ProgressRateStats;
import io.tapdata.Target;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public abstract class JdbcProgressRate implements DatabaseProgressRate {

	protected Logger logger = LogManager.getLogger(getClass());

	@Override
	public ProgressRateStats progressRateInfo(Job job, Connections sourceConn, Connections targetConn, MongoClient targetMongoClient, List<Target> targets) {
		ProgressRateStats progressRateStats = null;
//    Connection connection = null;
//    Statement stmt = null;
//    ResultSet resultSet = null;
//    try {
//      long sourceTS = 0;
//      long targetTS = 0;
//
//      String mappingTemplate = job.getMapping_template();
//      connection = JdbcUtil.createConnection(sourceConn);
//      stmt = connection.createStatement();
//
//      String syncType = job.getSync_type();
//      if (ConnectorConstant.SYNC_TYPE_INITIAL_SYNC.equals(syncType) || ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC.equals(syncType)) {
//        if (ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE.equals(mappingTemplate)) {
//          if (StringUtils.equalsAnyIgnoreCase(targetConn.getDatabase_type(), DatabaseTypeEnum.MONGODB.getType(), DatabaseTypeEnum.ALIYUN_MONGODB.getType())) {
//          } else if (CollectionUtils.isNotEmpty(targets)) {
//            for (Target target : targets) {
//
//              if (syncType.equals(ConnectorConstant.SYNC_TYPE_CDC) || syncType.equals(ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC)) {
//                long targetTs = target.getTargetLastChangeTimeStamp();
//                targetTS = targetTs > targetTS ? targetTs : targetTS;
//              }
//            }
//          }
//        } else {
//          if (StringUtils.equalsAnyIgnoreCase(targetConn.getDatabase_type(), DatabaseTypeEnum.MONGODB.getType(), DatabaseTypeEnum.ALIYUN_MONGODB.getType())) {
//          } else if (CollectionUtils.isNotEmpty(targets)) {
//            for (Target target : targets) {
//
//              if (syncType.equals(ConnectorConstant.SYNC_TYPE_CDC) || syncType.equals(ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC)) {
//                long targetTs = target.getTargetLastChangeTimeStamp();
//                targetTS = targetTs > targetTS ? targetTs : targetTS;
//              }
//            }
//          }
//        }
//      }
//      if (ConnectorConstant.SYNC_TYPE_CDC.equals(syncType) || ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC.equals(syncType)) {
//        targetTS = getTargetTimestamp(sourceConn, job.getOffset());
//        if (ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE.equals(mappingTemplate)) {
//          sourceTS = getDatabaseSourceTimestamp(sourceConn, stmt, targetTS);
//        } else {
//
//          List<Mapping> mappings = job.getMappings();
//          sourceTS = getTablesTimestamp(sourceConn, stmt, targetTS, mappings);
//        }
//      }
//
//      sourceTS = sourceTS < targetTS ? targetTS : sourceTS;
//
//      Map<String, Object> rowCount = new HashMap<>();
//      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//      Map<String, Object> ts = new HashMap<>();
//      ts.put(ProgressRateStats.SOURCE_FIELD, null);
//      ts.put(ProgressRateStats.TARGET_FIELD, null);
//      ts.put(ProgressRateStats.LAG_FIELD, null);
//      ts.put(ProgressRateStats.LAG_PER_FIELD, null);
//
//      if (sourceTS > 0) {
//        ts.put(ProgressRateStats.SOURCE_FIELD, sdf.format(new Date(new Long(String.valueOf(sourceTS)))));
//      }
//      if (targetTS > 0) {
//        ts.put(ProgressRateStats.TARGET_FIELD, sdf.format(new Date(new Long(String.valueOf(targetTS)))));
//        long lagSec = (sourceTS - targetTS) / 1000;
//        ts.put(ProgressRateStats.LAG_FIELD, lagSec);
//      }
//
//      progressRateStats = new ProgressRateStats(rowCount, ts);
//
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    } finally {
//
//      JdbcUtil.closeQuietly(resultSet);
//      JdbcUtil.closeQuietly(stmt);
//      JdbcUtil.closeQuietly(connection);
//
//    }

		return progressRateStats;
	}

	protected abstract long getTargetTimestamp(Connections sourceConn, Object offset) throws SQLException;

	protected abstract long getTablesTimestamp(Connections sourceConn, Statement stmt, long targetTS, List<Mapping> mappings) throws SQLException;

	protected abstract long getDatabaseSourceTimestamp(Connections sourceConn, Statement stmt, long targetTS) throws SQLException;

}
