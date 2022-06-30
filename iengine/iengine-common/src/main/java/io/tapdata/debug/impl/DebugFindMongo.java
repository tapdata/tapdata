package io.tapdata.debug.impl;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.tapdata.constant.MapUtil;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.dataflow.Stage;
import io.tapdata.debug.DebugConstant;
import io.tapdata.debug.DebugException;
import io.tapdata.debug.DebugFind;
import io.tapdata.debug.DebugUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DebugFindMongo implements DebugFind {

	private final static Logger logger = LogManager.getLogger(DebugFindMongo.class);

	private Job job;
	private Connections connections;
	private Stage stage;

	public DebugFindMongo(Job job, Connections connections, Stage stage) {
		this.job = job;
		this.connections = connections;
		this.stage = stage;
	}

	@Override
	public List<Map<String, Object>> backFindData(List<MessageEntity> msgs) throws DebugException {
		List<Map<String, Object>> datas = new ArrayList<>();
		int limit = job.getLimit() <= 0 ? DebugConstant.DEFAULT_DEBUG_LIMIT : job.getLimit();

		Map<String, List<Document>> toTablesFilter = new HashMap<>();
		Map<String, Integer> toTableCount = new HashMap<>();
		List<String> noPKTables = new ArrayList<>();

		for (MessageEntity msg : msgs) {
			String toTable = msg.getMapping().getTo_table();
			Mapping mapping = msg.getMapping();
			Map<String, Object> after = msg.getAfter();
			Document pkAnd = new Document();
			List<Map<String, String>> joinCondition = mapping.getJoin_condition();
			List<Document> orList;

			if (toTablesFilter.containsKey(toTable)) {
				orList = toTablesFilter.get(toTable);
			} else {
				if (CollectionUtils.isEmpty(joinCondition)) {
					logger.warn("DB clone, table {} has no primary key(s), will skip this table's target debug.", toTable);
					noPKTables.add(toTable);
					continue;
				}
				orList = new ArrayList<>();
				toTablesFilter.put(toTable, orList);
			}

			for (Map<String, String> condition : joinCondition) {
				for (String targetPk : condition.keySet()) {
					pkAnd.append(targetPk, MapUtil.getValueByKey(after, condition.getOrDefault(targetPk, null)));
					break;
				}
			}

			orList.add(pkAnd);

			if (toTableCount.containsKey(toTable)) {
				Integer count = toTableCount.get(toTable);
				if (count >= limit) {
					continue;
				} else {
					toTableCount.put(toTable, ++count);
				}
			} else {
				toTableCount.put(toTable, 0);
			}
		}

		try (
				MongoClient mongoClient = MongodbUtil.createMongoClient(connections)
		) {
			MongoDatabase database = mongoClient.getDatabase(MongodbUtil.getDatabase(connections));

			for (Map.Entry<String, List<Document>> entry : toTablesFilter.entrySet()) {
				MongoCollection<Document> collection = database.getCollection(entry.getKey());
				Document filter = new Document("$or", entry.getValue());
				logger.debug("Finding debug data in target mongodb, filter: {}", filter.toJson());

				try (
						MongoCursor<Document> iterator = collection.find(new Document(filter)).iterator()
				) {
					while (iterator.hasNext()) {
						Document document = iterator.next();

						DebugUtil.addDebugTags(document, stage.getId(), entry.getKey(), job);

						datas.add(document);
					}
				}
			}

		} catch (UnsupportedEncodingException e) {
			throw new DebugException("Find debug data in target mongodb error, message: " + e.getMessage(), e);
		}

		return datas;
	}
}
