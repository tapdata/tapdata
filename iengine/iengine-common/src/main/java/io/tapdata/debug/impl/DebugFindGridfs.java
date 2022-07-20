package io.tapdata.debug.impl;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.dataflow.Stage;
import io.tapdata.debug.DebugContext;
import io.tapdata.debug.DebugException;
import io.tapdata.debug.DebugFind;
import io.tapdata.debug.DebugUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DebugFindGridfs implements DebugFind {

	private DebugContext debugContext;
	private Stage stage;

	private final static Logger logger = LogManager.getLogger(DebugFindGridfs.class);

	public DebugFindGridfs(DebugContext debugContext, Stage stage) {
		this.debugContext = debugContext;
		this.stage = stage;
	}

	@Override
	public List<Map<String, Object>> backFindData(List<MessageEntity> msgs) throws DebugException {
		if (CollectionUtils.isEmpty(msgs)) return null;
		List<Map<String, Object>> datas = new ArrayList<>();
		Connections targetConn = debugContext.getTargetConn();
		Job job = debugContext.getJob();
		try (
				MongoClient mongoClient = MongodbUtil.createMongoClient(targetConn)
		) {
			MongoDatabase database = mongoClient.getDatabase(MongodbUtil.getDatabase(targetConn.getDatabase_uri()));
			String prefix = targetConn.getPrefix();
			MongoCollection<Document> filesCollection = database.getCollection(prefix + ".files");
			for (MessageEntity msg : msgs) {
				List<ObjectId> gridfsObjectIds = msg.getGridfsObjectIds();
				if (CollectionUtils.isEmpty(gridfsObjectIds)) continue;

				MongoCursor<Document> iterator = filesCollection.find(new Document("_id", new Document("$in", gridfsObjectIds))).iterator();
				while (iterator.hasNext()) {
					try {
						Document next = iterator.next();

						if (!next.containsKey("metadata")) continue;

						Document metadata = (Document) next.get("metadata");

						String metadataString = JSONUtil.map2JsonPretty(metadata);

						next.put("metadata", metadataString);

						DebugUtil.addDebugTags(next, stage.getId(), metadata.get("source_path").toString(), job);

						datas.add(next);

						/*if (metadata instanceof Map) {
							Map<String, Object> map = DebugUtil.constructFileMetaIntoDebugData((Map<String, Object>) metadata);
							DebugUtil.addDebugTags(map, stage.getId(), map.get("source_path").toString(), job);
							map.remove("source_path");
							datas.add(map);
						}*/
					} catch (Exception e) {
						logger.error("Handle debug data error: {}", e.getMessage(), e);
					}
				}
			}
		} catch (UnsupportedEncodingException e) {
			throw new DebugException(String.format("Mongodb uri is invalid: %s", debugContext.getTargetConn().getDatabase_uri()), e);
		} catch (Exception e) {
			throw new DebugException(String.format("Gridfs target debug error: %s.", e.getMessage()), e);
		}

		return datas;
	}
}
