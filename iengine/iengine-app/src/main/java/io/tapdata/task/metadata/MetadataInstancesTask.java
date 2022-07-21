package io.tapdata.task.metadata;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.task.Task;
import io.tapdata.task.TaskContext;
import io.tapdata.task.TaskResult;
import io.tapdata.task.TaskType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

@TaskType(type = "METADATA_INSTANCES_ANALYZE")
public class MetadataInstancesTask implements Task {
	private TaskContext taskContext;

	private Logger logger = LogManager.getLogger(getClass());

	@Override
	public void initialize(TaskContext taskContext) {
		this.taskContext = taskContext;
	}

	@Override
	public void execute(Consumer<TaskResult> callback) {
		TaskResult taskResult = new TaskResult();
		taskResult.setPassResult();
		try {
			ClientMongoOperator clientMongoOperator = taskContext.getClientMongoOperator();
			MongoTemplate mongoTemplate = clientMongoOperator.getMongoTemplate();
			if (mongoTemplate != null) {
				long startTS = System.currentTimeMillis();

				for (Document collectionInfo : mongoTemplate.getDb().listCollections()) {
					String collectionName = collectionInfo.getString("name");
					if (ConnectorConstant.METADATA_INSTANCE_COLLECTION.equals(collectionName)) {
						logger.info("Start initial analysis metadata instance from {}.", collectionName);
						Document filter = new Document();
						filter.append("$or", Arrays.asList(
								new Document("meta_type", "job")
						));
						Document projection = new Document();

						try (MongoCursor<Document> cursor = mongoTemplate.getDb().getCollection(collectionName).find(filter).projection(projection).iterator()) {

							while (cursor.hasNext()) {
								Document record = cursor.next();
								metadataStats(collectionName, record, "insert");
							}

						} catch (Exception e) {
							logger.error("Initial analysis metadata from {} failed.", collectionName, e);
						}
					}
				}

				logger.info("Start listening change.");
				try (
						MongoCursor<ChangeStreamDocument<Document>> metadataWatch = getMatadataWatch(mongoTemplate);
				) {

					while ("scheduling".equals(taskContext.getScheduleTask().getStatus())) {
						ChangeStreamDocument<Document> matadataEvent = metadataWatch.tryNext();
						if (matadataEvent != null) {
							distributeProcessors(matadataEvent);
						}

						long currTS = System.currentTimeMillis();
						if (currTS - startTS > 600000) {
							break;
						}
					}
				} catch (Exception e) {
					logger.error("Change stream listening change failed.", e);
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			taskResult.setFailedResult(e.getMessage());
		}

		callback.accept(taskResult);
	}

	private MongoCursor<ChangeStreamDocument<Document>> getMatadataWatch(MongoTemplate mongoTemplate) {
		return mongoTemplate.getDb().getCollection(ConnectorConstant.METADATA_INSTANCE_COLLECTION).watch(Arrays.asList(
				new Document("$match",
						new Document("$or", Arrays.asList(
								new Document("operationType", "insert"),
								new Document("operationType", "replace"),
								new Document("operationType", "delete"),
								new Document("fullDocument.is_deleted", false),
								new Document("fullDocument.is_deleted", new Document("$exists", false)),
								new Document("$or", Arrays.asList(
										new Document("updateDescription.updatedFields.field_alias", new Document("$exists", true))
								)).append("operationType", "update")
						))
				)
		)).fullDocument(FullDocument.UPDATE_LOOKUP).iterator();
	}

	private void distributeProcessors(ChangeStreamDocument<Document> event) {

		String operationType = event.getOperationType().getValue();
		Document fullDocument = event.getFullDocument();
		String coll = event.getNamespace().getCollectionName();

		switch (operationType) {
			case "delete":
				Document documentKey = new Document();
				documentKey.putAll(event.getDocumentKey());
				metadataStats(coll, documentKey, operationType);
				break;
			default:
				metadataStats(coll, fullDocument, operationType);

				break;
		}

	}

	private void metadataStats(String collectionName, Document record, String operationType) {
		ClientMongoOperator clientMongoOperator = taskContext.getClientMongoOperator();
		try {

			List<Document> metadataRecords = process(collectionName, record, operationType, clientMongoOperator);
			if (CollectionUtils.isNotEmpty(metadataRecords)) {
				for (Document metadataRecord : metadataRecords) {
					upsertMetadata(metadataRecord);
				}
			}

		} catch (Exception e) {
			logger.error("Call process failed.", e);
		}
	}

	private List<Document> process(String collectionName, Document record, String operationType, ClientMongoOperator clientMongoOperator) {

		if (collectionName.equals("MetadataInstances") &&
				(!record.containsKey("is_deleted") || !record.getBoolean("is_deleted"))
		) {
			if (
					StringUtils.equalsAny(record.getString("meta_type"), "job")
			) {

				MongoTemplate mongoTemplate = taskContext.getClientMongoOperator().getMongoTemplate();
				try {

					if (record.containsKey("source") && record.get("source") != null) {
						if (record.getString("meta_type").equals("job")) {
							Document source = record.get("source", Document.class);
							Object sourceId = source.get("_id");
							if (sourceId instanceof String) {
								sourceId = new ObjectId((String) sourceId);
							}

							long jobCount = mongoTemplate.getDb().getCollection(ConnectorConstant.JOB_COLLECTION).countDocuments(
									new Document("_id", sourceId)
							);
							if (jobCount <= 0) {

								mongoTemplate.getDb().getCollection(ConnectorConstant.METADATA_INSTANCE_COLLECTION).updateOne(
										new Document().append("_id", record.getObjectId("_id")),
										new Document("$set", new Document()
												.append("is_deleted", true)
										).append("$unset", new Document()
												.append("lienage", 1).append("field_alias", 1)
												.append("fields", 1).append("schema", 1)
												.append("indexes", 1)
										)
								);
								mongoTemplate.getDb().getCollection(ConnectorConstant.METADATA_INSTANCE_COLLECTION).updateMany(
										new Document(),
										new Document().append("$pull", new Document()
												.append("lignage", new Document()
														.append("qualified_name", record.getString("qualified_name"))
												)
										)
								);

								record.put("is_deleted", true);
								logger.info("Metadata already deleted name {} meta type {} id {}.", record.getString("original_name"), record.getString("meta_type"), record.getObjectId("_id").toHexString());
							}
						}
					}

				} catch (Exception e) {
					logger.error("Delete database or collection metadata {}  meta type {}  failed.", record.getString("original_name"), record.getString("meta_type"), e);
				}
			}

		}
		return null;
	}

	private void formatMetadataRecord(Document metadataRecord) {
		metadataRecord.put("last_updated", new Date());

		if (!metadataRecord.containsKey("is_deleted")) {
			metadataRecord.put("is_deleted", false);
		}
	}

	private void upsertMetadata(Document metadata) {
		formatMetadataRecord(metadata);
		MongoTemplate mongoTemplate = taskContext.getClientMongoOperator().getMongoTemplate();
		try {
			List lienage = (List) metadata.get("lienage");
			metadata.remove("lienage");

			Document updateOperattion = new Document("$set", metadata);
			if (CollectionUtils.isNotEmpty(lienage)) {
				updateOperattion.append("$addToSet", new Document("lienage", new Document("$each", lienage)));
			}

			mongoTemplate.getDb().getCollection(ConnectorConstant.METADATA_INSTANCE_COLLECTION).updateOne(
					new Document("qualified_name", metadata.getString("qualified_name")),
					updateOperattion,
					new UpdateOptions().upsert(true)
			);
		} catch (Exception e) {
			logger.error("Upsert metadata record [ qualified_name {}, name {}, meta type {}] failed.",
					metadata.getString("qualified_name"),
					metadata.getString("original_name"),
					metadata.getString("meta_type"), e
			);
		}
	}
}
