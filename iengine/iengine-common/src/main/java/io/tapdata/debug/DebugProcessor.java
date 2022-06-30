package io.tapdata.debug;

import com.tapdata.constant.MapUtil;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.debug.impl.ClientDebug;
import io.tapdata.debug.impl.DebugFindGridfs;
import io.tapdata.debug.impl.DebugFindJdbc;
import io.tapdata.debug.impl.DebugFindMongo;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DebugProcessor {

	private final static Logger logger = LogManager.getLogger(DebugProcessor.class);

	private DebugContext debugContext;
	private Map<String, String> fromToTabelMap;
	private Map<String, List<Stage>> fromTableStagesMap;
	private boolean debug = false;
	private Debug debugDao;

	public DebugProcessor(DebugContext debugContext) throws DebugException {
		this.debugContext = debugContext;

		this.debugDao = new ClientDebug(debugContext.getClientMongoOperator());

		if (baseValidate()) {
			if (debugContext.getJob().isDebug()
					&& StringUtils.isNotBlank(debugContext.getJob().getDataFlowId())) {
				init();
				this.debug = true;
			}
		} else {
			throw new DebugException("Init debug processor error, missing input parameter(s).");
		}
	}

	private void init() throws DebugException {
		initFromTableMap();
		logger.debug("Init debug processor, fromToTabelMap: {}.");
	}

	public static void clearDebugs(String dataFlowId, ClientMongoOperator clientMongoOperator) throws DebugException {
		Debug debug = new ClientDebug(clientMongoOperator);
		debug.clearDebugData(dataFlowId);
		debug.clearGridfsFiles(dataFlowId);
	}

	private void initFromTableMap() {
		fromToTabelMap = new HashMap<>();
		fromTableStagesMap = new HashMap<>();
		List<Mapping> mappings = debugContext.getJob().getMappings();
		for (Mapping mapping : mappings) {
			if (!fromToTabelMap.containsKey(mapping.getFrom_table())) {
				fromToTabelMap.put(mapping.getFrom_table(), mapping.getTo_table());
				fromTableStagesMap.put(mapping.getFrom_table(), mapping.getStages());
			}
		}
	}

	public void debugProcess(Stage stage, List<MessageEntity> msgs) throws DebugException {
		if (processValidate(msgs, stage)) {
			writeDebugResults(msgs, stage);
		} else {
			throw new DebugException(String.format("Debug process error, missing input parameter(s), stage: %s, msgs size: %s, context: %s.",
					stage, msgs.size(), debugContext));
		}
	}

	private boolean baseValidate() {
		return debugContext.getJob() != null && debugContext.getClientMongoOperator() != null;
	}

	private boolean processValidate(List<MessageEntity> msgs, Stage stage) {
		return baseValidate()
				&& CollectionUtils.isNotEmpty(msgs)
				&& stage != null;
	}

	private void writeDebugResults(List<MessageEntity> msgs, Stage stage) throws DebugException {
		List<Map<String, Object>> datas = new ArrayList<>();
		ZoneId zoneId;
		if (stage.getSourceOrTarget().equals("target")) {
			DebugFind debugFind = null;
			DatabaseTypeEnum databaseTypeEnum = DatabaseTypeEnum.fromString(debugContext.getTargetConn().getDatabase_type());
			zoneId = debugContext.getTargetConn().getZoneId() == null ? ZoneId.of("UTC") : debugContext.getTargetConn().getZoneId();
			switch (databaseTypeEnum) {
				case MONGODB:
				case ALIYUN_MONGODB:
					debugFind = new DebugFindMongo(debugContext.getJob(), debugContext.getTargetConn(), stage);
					break;
				case ORACLE:
				case DB2:
				case GAUSSDB200:
				case GBASE8S:
				case MSSQL:
				case ALIYUN_MSSQL:
				case MYSQL:
				case MARIADB:
				case DAMENG:
				case MYSQL_PXC:
				case POSTGRESQL:
				case ALIYUN_POSTGRESQL:
				case GREENPLUM:
				case SYBASEASE:
				case KUNDB:
				case ADB_MYSQL:
				case ALIYUN_MYSQL:
				case ALIYUN_MARIADB:
				case ADB_POSTGRESQL:
					debugFind = new DebugFindJdbc(debugContext.getJob(), debugContext.getSourceConn(), debugContext.getTargetConn(), stage);
					break;
				case GRIDFS:
					debugFind = new DebugFindGridfs(debugContext, stage);
					break;
				case MEM_CACHE:
					break;
				default:
					throw new DebugException("Unsupported database type " + databaseTypeEnum.getType());
			}
			if (debugFind != null) {
				datas.addAll(debugFind.backFindData(msgs));
			}
		} else {
			zoneId = debugContext.getSourceConn().getZoneId() == null ? ZoneId.of("UTC") : debugContext.getSourceConn().getZoneId();
			try {
				datas = getDatas(msgs, stage);
			} catch (InstantiationException | IllegalAccessException e) {
				throw new DebugException(String.format("Deep clone data error, message: %s", e.getMessage()), e);
			}
		}

		DebugUtil.handleDebugData(datas, zoneId);
		debugDao.writeDebugData(datas);
	}

	private List<Map<String, Object>> getDatas(List<MessageEntity> msgs, Stage stage) throws InstantiationException, IllegalAccessException {
		List<Map<String, Object>> datas = new ArrayList<>();
		Map<String, Integer> tableCount = new HashMap<>();
		int limit = debugContext.getJob().getLimit() <= 0 ? DebugConstant.DEFAULT_DEBUG_LIMIT : debugContext.getJob().getLimit();
		if (CollectionUtils.isNotEmpty(msgs)) {
			for (MessageEntity msg : msgs) {
				if (msg != null) {
					String tableName = msg.getTableName();
					String stageId = stage.getId();
					if (limit <= tableCount.getOrDefault(tableName, 0)) continue;
					Map<String, Object> map = new HashMap<>();
					if (MapUtils.isEmpty(msg.getAfter())) continue;
					MapUtil.deepCloneMap(msg.getAfter(), map);
					DebugUtil.addDebugTags(map, stageId, tableName, debugContext.getJob());
					datas.add(map);
					tableCount.put(tableName, tableCount.getOrDefault(tableName, 0) + 1);
				}
			}
		}
		return datas;
	}

	public enum StageType {
		SOURCE,
		JS_PROCESSOR,
		TARGET,
		;
	}

	public DebugContext getDebugContext() {
		return debugContext;
	}
}
