package io.tapdata.debug.impl;

import com.tapdata.entity.DataQualityTag;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.debug.Debug;
import io.tapdata.debug.DebugConstant;
import io.tapdata.debug.DebugException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClientDebug implements Debug {

	private final static Logger logger = LogManager.getLogger(ClientDebug.class);

	private ClientMongoOperator clientMongoOperator;

	public ClientDebug(ClientMongoOperator clientMongoOperator) throws DebugException {
		if (clientMongoOperator == null)
			throw new DebugException("clientMongoOperator is null.", new IllegalArgumentException());

		this.clientMongoOperator = clientMongoOperator;
	}

	@Override
	public void clearDebugData(String dataFlowId) throws DebugException {
		try {
			logger.debug("Start clear debug data, DataFlow id: {}, collection name: {}.", dataFlowId, DebugConstant.DEBUG_COLLECTION_NAME);
			Map<String, Object> params = new HashMap<String, Object>() {{
				put(DataQualityTag.SUB_COLUMN_NAME + "." + DebugConstant.SUB_DATAFLOW_ID, new HashMap<String, Object>() {{
					put("regexp", "^" + dataFlowId + "$");
				}});
			}};

			clientMongoOperator.deleteAll(params, DebugConstant.DEBUG_COLLECTION_NAME);
		} catch (Exception e) {
			throw new DebugException("Clear data flow debug data(s) error, message: " + e.getMessage(), e);
		}
	}

	@Override
	public void clearDebugLogs(String dataFlowId) throws DebugException {
		try {
			logger.debug("Start clear debug logs, DataFlow id: {}, collection name: {}.", dataFlowId, DebugConstant.LOGS_COLLECTION_NAME);
			Map<String, Object> params = new HashMap<String, Object>() {{
				put("contextMap.dataFlowId", new HashMap<String, Object>() {{
					put("regexp", "^" + dataFlowId + "$");
				}});
			}};

			clientMongoOperator.deleteAll(params, DebugConstant.LOGS_COLLECTION_NAME);
		} catch (Exception e) {
			throw new DebugException("Clear data flow debug log(s) error, message: " + e.getMessage(), e);
		}
	}

	@Override
	public void clearGridfsFiles(String dataFlowId) throws DebugException {
		// no need, do nothing
	}

	@Override
	public void writeDebugData(List<Map<String, Object>> datas) throws DebugException {
		if (CollectionUtils.isNotEmpty(datas)) {
			try {
				clientMongoOperator.insertList(datas, DebugConstant.DEBUG_COLLECTION_NAME);
			} catch (Exception e) {
				throw new DebugException("Write debug datas error: " + e.getMessage(), e);
			}
		}
	}
}
