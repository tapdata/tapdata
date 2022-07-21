package com.tapdata.cache;

import com.tapdata.constant.DataFlowStageUtil;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.ScriptConnection;
import com.tapdata.processor.ScriptUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 缓存查不到的情况下根据源库的连接信息进行查库获取数据
 */
public class MongoRowsGetter implements IDataSourceRowsGetter {

	private Logger logger = LogManager.getLogger(ICacheGetter.class);

	/**
	 * 缓存源库的连接信息
	 */
	private Connections sourceConnection;

	private ScriptConnection scriptConnection;

	private DataFlowCacheConfig config;

	/**
	 * - key: stage id
	 * - value:
	 * -- key: field name
	 * -- value: projection
	 */
	private Map<String, Map<String, Integer>> stageFieldProjection;

	public MongoRowsGetter(DataFlowCacheConfig config) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		this.config = config;
		this.sourceConnection = config.getSourceConnection();
		this.scriptConnection = ScriptUtil.initScriptConnection(sourceConnection);
		Stage sourceStage = config.getSourceStage();
		Map<String, Integer> fieldProjection = DataFlowStageUtil.stageToFieldProjection(sourceStage);
		if (MapUtils.isNotEmpty(fieldProjection)) {
			stageFieldProjection = new ConcurrentHashMap<String, Map<String, Integer>>() {{
				put(sourceStage.getId(), fieldProjection);
			}};
		}
	}

	@Override
	public List<Map<String, Object>> getRows(Object[] keys) {
		List<Map<String, Object>> results = null;

		if (scriptConnection != null) {

			String database = MongodbUtil.getDatabase(sourceConnection);
			results = ScriptUtil.executeMongoQuery(
					scriptConnection,
					database,
					this.config.getTableName(),
					this.config.getCacheKeys(),
					keys
			);
		}

		return results;
	}
}
