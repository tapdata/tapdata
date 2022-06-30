package io.tapdata.inspect.cdc.operator;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.inspect.InspectCdcWinData;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.util.Map;

/**
 * 增量源操作实例 - 共享日志
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/11/3 上午11:49 Create
 */
public class ShareLogInspectCdcOperator extends AbsInspectCdcOperator {
	private final static Logger logger = LogManager.getLogger(ShareLogInspectCdcOperator.class);
	private String logDataFlowId;
	private String logTableName;
	private String tableName;
	private MongoClient mongoClient;
	private MongoDatabase database;

	public ShareLogInspectCdcOperator(String logDataFlowId, Connections logConn, String logTableName, String tableName) throws UnsupportedEncodingException {
		super(true);
		this.logDataFlowId = logDataFlowId;
		this.logTableName = logTableName;
		this.tableName = tableName;

		mongoClient = MongodbUtil.createMongoClient(logConn);
		database = mongoClient.getDatabase(logConn.getDatabase_name());
	}

	@Override
	public long count(InspectCdcWinData cdcWinData) {
		Document filters = new Document();
		filters.append("timestamp", new Document()
						.append("$gte", cdcWinData.getWinBegin().toEpochMilli())
						.append("$lt", cdcWinData.getWinEnd().toEpochMilli())
				)
				.append("dataFlowId", logDataFlowId)
				.append("data.TABLE_NAME", tableName);
		long counts = database.getCollection(logTableName).countDocuments(filters);
		return counts;
	}

	@Override
	public Instant lastEventDate() {
		Map record = database.getCollection(logTableName).find(new Document()
						.append("dataFlowId", logDataFlowId)
						.append("data.TABLE_NAME", tableName)
				, Map.class
		).sort(new Document()
				.append("timestamp", -1)
		).limit(1).first();
		if (null == record || !record.containsKey("timestamp")) return null;

		long timestamp = (long) record.get("timestamp");
		return Instant.ofEpochMilli(timestamp);
	}

	@Override
	public void close() throws Exception {
		if (null != mongoClient) {
			mongoClient.close();
		}
	}

	public static ShareLogInspectCdcOperator buildBySourceConnection(ClientMongoOperator clientMongoOperator, Connections sourceConn, String tableName) throws UnsupportedEncodingException {
//    Map logConnectionInfo;
//    try {
//      Set<String> tables = new HashSet<>();
//      tables.add(tableName);
//      logConnectionInfo = LogReaderUtil.findLogConnection(clientMongoOperator, sourceConn, tables);
//    } catch (Exception e) {
//      throw new InspectCdcNonsupportException("Find log collect connection failed", e);
//    }
//    if (null == logConnectionInfo) throw new InspectCdcNonsupportException("Not found log collect connection");
//
//    return new ShareLogInspectCdcOperator(
//      (String) logConnectionInfo.get("dataFlowId"),
//      (Connections) logConnectionInfo.get("connection"),
//      (String) logConnectionInfo.get("tableName"),
//      tableName
//    );
		return null;
	}
}
