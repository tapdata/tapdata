package io.tapdata.task.metadata;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.ClassScanner;
import io.tapdata.task.Task;
import io.tapdata.task.TaskContext;
import io.tapdata.task.TaskResult;
import io.tapdata.task.metadata.processor.MetadataProcessor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

//@TaskType(type = "META_DATA_ANALYZER")
public class MetadataAnalyzerTask implements Task {

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

				List<MetadataProcessor> metadataProcessors = scanMetadataProcessors();

				logger.info("Found metadata processor {} instances.", metadataProcessors.size());
				for (Document collectionInfo : mongoTemplate.getDb().listCollections()) {
					String collectionName = collectionInfo.getString("name");
					if (ConnectorConstant.JOB_COLLECTION.equals(collectionName) ||
							ConnectorConstant.CONNECTION_COLLECTION.equals(collectionName) ||
							ConnectorConstant.MODULES_COLLECTION.equals(collectionName)) {
						logger.info("Start initial analysis metadata instance from {}.", collectionName);
						Document filter = new Document();
						Document projection = new Document();
						if (collectionName.equals("Jobs")) {
							projection.append("editorData", 0);
						}


						try (MongoCursor<Document> cursor = mongoTemplate.getDb().getCollection(collectionName).find(filter).projection(projection).iterator()) {

							while (cursor.hasNext()) {
								Document record = cursor.next();
								metadataStats(metadataProcessors, collectionName, record, "insert");
							}

						} catch (Exception e) {
							logger.error("Initial analysis metadata from {} failed.", collectionName, e);
						}
					}
				}

				logger.info("Start listening change.");
				try (
						MongoCursor<ChangeStreamDocument<Document>> connecctionWatch = getConnecctionWatch(mongoTemplate);
						MongoCursor<ChangeStreamDocument<Document>> jobWatch = getJobWatch(mongoTemplate);
						MongoCursor<ChangeStreamDocument<Document>> modulesWatch = getModulesWatch(mongoTemplate);
						MongoCursor<ChangeStreamDocument<Document>> scheduleTasksWatch = getScheduleTasksWatch(mongoTemplate);
				) {

					while ("scheduling".equals(taskContext.getScheduleTask().getStatus())) {
						ChangeStreamDocument<Document> connectionEvent = connecctionWatch.tryNext();
						if (connectionEvent != null) {
							distributeProcessors(connectionEvent, metadataProcessors);
						}

						ChangeStreamDocument<Document> jobEvent = jobWatch.tryNext();
						if (jobEvent != null) {
							distributeProcessors(jobEvent, metadataProcessors);
						}

						ChangeStreamDocument<Document> modulesEvent = modulesWatch.tryNext();
						if (modulesEvent != null) {
							distributeProcessors(modulesEvent, metadataProcessors);
						}

						ChangeStreamDocument<Document> scheduleTaskEvent = scheduleTasksWatch.tryNext();
						if (scheduleTaskEvent != null) {
							distributeProcessors(scheduleTaskEvent, metadataProcessors);
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

	private MongoCursor<ChangeStreamDocument<Document>> getModulesWatch(MongoTemplate mongoTemplate) {
		return mongoTemplate.getDb().getCollection(ConnectorConstant.MODULES_COLLECTION).watch().fullDocument(FullDocument.UPDATE_LOOKUP).iterator();
	}

	private MongoCursor<ChangeStreamDocument<Document>> getScheduleTasksWatch(MongoTemplate mongoTemplate) {
		return mongoTemplate.getDb().getCollection(ConnectorConstant.SCHEDULE_TASK_COLLECTION).watch(Arrays.asList(
				new Document("$match",
						new Document("operationType", "update")
								.append("updateDescription.updatedFields.status", "paused")
				)
		)).fullDocument(FullDocument.UPDATE_LOOKUP).iterator();
	}

	private MongoCursor<ChangeStreamDocument<Document>> getJobWatch(MongoTemplate mongoTemplate) {
		return mongoTemplate.getDb().getCollection(ConnectorConstant.JOB_COLLECTION).watch(Arrays.asList(
				new Document("$match", new Document("$or", Arrays.asList(
						new Document("operationType", "delete"),
						new Document("$or", Arrays.asList(
								new Document("operationType", "insert"),
								new Document("operationType", "replace")
						)).append("fullDocument.status", new Document("$ne", "draft")),
						new Document("operationType", "update")
								.append("fullDocument.status", new Document("$ne", "draft"))
								.append("$or", Arrays.asList(
										new Document("updateDescription.updatedFields.stats", new Document("$exists", true)),
										new Document("updateDescription.updatedFields.stats.total.source_received", new Document("$exists", true)),
										new Document("updateDescription.updatedFields.stats.total.processed", new Document("$exists", true)),
										new Document("updateDescription.updatedFields.stats.total.total_updated", new Document("$exists", true)),
										new Document("updateDescription.updatedFields.stats.total.total_data_quality", new Document("$exists", true)),
										new Document("updateDescription.updatedFields.stats.total.total_data_size", new Document("$exists", true)),
										new Document("updateDescription.updatedFields.name", new Document("$exists", true)),
										new Document("updateDescription.updatedFields.connections", new Document("$exists", true)),
										new Document("updateDescription.updatedFields.mappings", new Document("$exists", true)),
										new Document("updateDescription.updatedFields.deployment", new Document("$exists", true)),
										new Document("updateDescription.updatedFields.mapping_template", new Document("$exists", true)),
										new Document("updateDescription.updatedFields.progressRateStats", new Document("$exists", true)),
										new Document("updateDescription.updatedFields.row_count", new Document("$exists", true)),
										new Document("updateDescription.updatedFields.ts", new Document("$exists", true))
								))
				))),
				new Document("$project", new Document("editorData", 0))
		)).fullDocument(FullDocument.UPDATE_LOOKUP).iterator();
	}

	private MongoCursor<ChangeStreamDocument<Document>> getConnecctionWatch(MongoTemplate mongoTemplate) {
		return mongoTemplate.getDb().getCollection(ConnectorConstant.CONNECTION_COLLECTION).watch(Arrays.asList(
				new Document("$match", new Document("$or", Arrays.asList(
						new Document("operationType", "delete"),
						new Document("operationType", "insert"),
						new Document("operationType", "replace"),
						new Document("operationType", "update")
								.append("$or", Arrays.asList(
										new Document("updateDescription.updatedFields.table_alias", new Document("$exists", true)),
										new Document("updateDescription.updatedFields.status", new Document("$exists", true))
										//, new Document("updateDescription.updatedFields.listtags", new Document("$exists", true))
								))
				)))
		)).fullDocument(FullDocument.UPDATE_LOOKUP).iterator();
	}

	private void distributeProcessors(ChangeStreamDocument<Document> event, List<MetadataProcessor> metadataProcessors) {

		String operationType = event.getOperationType().getValue();
		Document fullDocument = event.getFullDocument();
		String coll = event.getNamespace().getCollectionName();

		switch (operationType) {
			case "delete":
				Document documentKey = new Document();
				documentKey.putAll(event.getDocumentKey());
				metadataStats(metadataProcessors, coll, documentKey, operationType);
				break;
			default:
				metadataStats(metadataProcessors, coll, fullDocument, operationType);

				break;
		}
	}

	private List<MetadataProcessor> scanMetadataProcessors() throws IllegalAccessException, InstantiationException {
		List<MetadataProcessor> metadataProcessors = new ArrayList<>();
		List<Class<? extends MetadataProcessor>> allClassByInterface = new ArrayList<>();

		try {
			Set<Class<?>> matchComponents = ClassScanner.findMatchComponents(MetadataProcessor.class.getPackage().getName());
			if (CollectionUtils.isNotEmpty(matchComponents)) {
				allClassByInterface.clear();
				for (Class<?> matchComponent : matchComponents) {
					Class<?>[] interfaces = matchComponent.getInterfaces();
					if (interfaces != null && interfaces.length > 0) {

						for (Class<?> anInterface : interfaces) {
							if (anInterface.getName().equals(MetadataProcessor.class.getName())) {
								allClassByInterface.add((Class<? extends MetadataProcessor>) matchComponent);
								break;
							}
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (CollectionUtils.isNotEmpty(allClassByInterface)) {
			for (Class<? extends MetadataProcessor> aClass : allClassByInterface) {
				metadataProcessors.add(aClass.newInstance());
			}
		}
		return metadataProcessors;
	}

	private void formatMetadataRecord(Document metadataRecord) {
		metadataRecord.put("last_updated", new Date());

		if (metadataRecord.containsKey("source")) {
			Document source = (Document) metadataRecord.get("source");
			try {
				if (source.containsKey("_id")) {
					Object sourceId = source.get("_id");
					if (sourceId instanceof ObjectId) {
						source.put("_id", ((ObjectId) sourceId).toHexString());
					}
				}
			} catch (Exception e) {
				// do nothing
			}
		}
	}

	private void upsertMetadata(Document metadata) {
		formatMetadataRecord(metadata);
		ClientMongoOperator clientMongoOperator = taskContext.getClientMongoOperator();
		MongoTemplate mongoTemplate = clientMongoOperator.getMongoTemplate();
		try {
			List lienage = (List) metadata.get("lienage");
			metadata.remove("lienage");

			Document updateOperattion = new Document("$set", metadata);
			if (CollectionUtils.isNotEmpty(lienage)) {
				updateOperattion.append("$addToSet", new Document("lienage", new Document("$each", lienage)));
			}

			List fields_lienage = (List) metadata.get("fields_lienage");
			metadata.remove("fields_lienage");
			if (CollectionUtils.isNotEmpty(fields_lienage)) {
				updateOperattion.append("$addToSet", new Document("fields_lienage", new Document("$each", fields_lienage)));
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

	private void metadataStats(List<MetadataProcessor> metadataProcessors, String collectionName, Document record, String operationType) {
		ClientMongoOperator clientMongoOperator = taskContext.getClientMongoOperator();
		if (CollectionUtils.isNotEmpty(metadataProcessors)) {
			for (MetadataProcessor metadataProcessor : metadataProcessors) {
				try {

					List<Document> metadataRecords = metadataProcessor.process(collectionName, record, operationType, clientMongoOperator);
					if (CollectionUtils.isNotEmpty(metadataRecords)) {
						for (Document metadataRecord : metadataRecords) {
							upsertMetadata(metadataRecord);
						}
					}

				} catch (Exception e) {
					logger.error("Call {} process failed.", metadataProcessor.getClass(), e);
				}
			}
		}
	}

}
