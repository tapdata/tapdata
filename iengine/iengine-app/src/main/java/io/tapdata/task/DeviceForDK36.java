package io.tapdata.task;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.constant.ConnectorConstant;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@TaskType(type = "DEVICE_STATS")
public class DeviceForDK36 implements Task {

	private final static Logger logger = LogManager.getLogger(DeviceForDK36.class);
	private final static String TARGET_COLLECTION = "Dk36DeviceInfo";

	private TaskContext context;
	private int lastHeartbeat;
	private ObjectId lastObjectId;
	private Map<String, Document> updateDocsBuffer;
	private BigDecimal currentTime;

	@Override
	public void initialize(TaskContext taskContext) {
		this.context = taskContext;

		updateDocsBuffer = new HashMap<>();
	}

	@Override
	public void execute(Consumer<TaskResult> callback) {
		lastHeartbeat = context.getSettingService().getInt("lastHeartbeat", 60);
		MongoTemplate mongoTemplate = context.getClientMongoOperator().getMongoTemplate();
		TaskResult taskResult = new TaskResult();
		taskResult.setPassResult();

		if (mongoTemplate != null) {

			try {
				int row = 0;

				for (Document system : findLastSystemsInWorker()) {
					updateDocsBuffer.put((String) system.get("_id"), system);
					lastObjectId = system.getObjectId("id");
				}

				for (Document system : updateDocsBuffer.values()) {
					setCurrentTime();
					Document deviceInfo = getDeviceInfo(system);

					deviceInfo = upperKeysInDocument(deviceInfo);

					UpdateResult updateResult = mongoTemplate.getDb().getCollection(TARGET_COLLECTION).updateOne(
							new Document("MONITORIP", deviceInfo.get("MONITORIP")),
							new Document("$set", deviceInfo)
									.append("$currentDate", new Document("UPDATETIME", true)),
							new UpdateOptions().upsert(true)
					);

					row += updateResult.getModifiedCount();
					if (updateResult.getUpsertedId() != null) {
						row++;
					}
				}

				logger.info("Finished stats device info(s), number of servers : {}\n  Servers ip: {}", row, String.join(" | ", updateDocsBuffer.keySet()));
			} catch (Exception e) {
				logger.error("Stats device info error: {}", e.getMessage(), e);
				taskResult.setFailedResult("Stats device info error: " + e.getMessage());
			}

		} else {
			taskResult.setFailedResult("mongo template is null.");
		}

		callback.accept(taskResult);
	}

	private AggregateIterable<Document> findLastSystemsInWorker() {
		MongoTemplate mongoTemplate = context.getClientMongoOperator().getMongoTemplate();

		Document match = new Document("worker_type", "system");

		if (lastObjectId != null) {
			match.append("_id", new Document("$gt", lastObjectId));
		}

		List aggregate = Arrays.asList(
				new Document("$match", match),
				new Document("$group",
						new Document("_id", "$worker_ip")
								.append("ping_time", new Document("$max", "$ping_time"))
								.append("info", new Document("$last", "$info"))
								.append("id", new Document("$last", "$_id"))
				));

		AggregateIterable<Document> systems = mongoTemplate.getDb().getCollection(ConnectorConstant.WORKER_COLLECTION).aggregate(aggregate);

		return systems;
	}

	private int getDeviceStatus(Object pingTime) {
		return currentTime
				.subtract(new BigDecimal(pingTime.toString()))
				.compareTo(new BigDecimal(lastHeartbeat).multiply(new BigDecimal(1000))) == 1 ? 0 : 1;
	}

	private Document getCollectContent(Document info) {
		Document collectContent = new Document();
		if (MapUtils.isNotEmpty(info)) {
			if (info.containsKey("cpu") && info.get("cpu") instanceof Document) {
				Document cpu = (Document) info.get("cpu");
				if (cpu.containsKey("count")) {
					collectContent.append("cores", cpu.get("count"));
				}

				if (cpu.containsKey("usage")) {
					collectContent.append("cpu", cpu.get("usage"));
				}
			}

			if (info.containsKey("mem") && info.get("mem") != null && info.get("mem") instanceof Document) {
				Document mem = (Document) info.get("mem");
				if (mem.containsKey("used") && mem.containsKey("total")) {
					BigDecimal usedPercentage;

					BigDecimal used = new BigDecimal(mem.get("used") + "");
					BigDecimal total = new BigDecimal(mem.get("total") + "");

					usedPercentage = used.divide(total, 2, BigDecimal.ROUND_HALF_UP);

					collectContent.append("mem", usedPercentage);
				}
			}

			if (info.containsKey("disk") && info.get("disk") instanceof List) {
				collectContent.append("storage", info.get("disk"));
			}

			if (info.containsKey("networks")) {
				Object networks = info.get("networks");
				if (networks instanceof List) {
					collectContent.append("network", networks);
				}
			}
		}

		return collectContent;
	}

	private Document getDeviceInfo(Document system) {
		Document deviceInfo = new Document();

		if (MapUtils.isNotEmpty(system)) {
			deviceInfo.append("monitorip", system.get("_id"));
			deviceInfo.append("devicestatus", getDeviceStatus(system.get("ping_time")));
			if (system.containsKey("info") && system.get("info") instanceof Document) {
				deviceInfo.append("collectcontent", getCollectContent((Document) system.get("info")));
			}
			List<Document> softwares = new ArrayList<>();
			deviceInfo.append("software", softwares);

			MongoTemplate mongoTemplate = context.getClientMongoOperator().getMongoTemplate();
			FindIterable<Document> softwareFind = mongoTemplate.getDb().getCollection(ConnectorConstant.WORKER_COLLECTION).find(
					new Document("worker_ip", system.get("_id"))
							.append("$and", Arrays.asList(
									new Document("worker_type", new Document("$ne", "system")),
									new Document("worker_type", new Document("$ne", "Device"))
							))
			).projection(new Document("hostname", 1).append("worker_type", 1).append("ping_time", 1));

			for (Document document : softwareFind) {
				Document software = new Document();

				software.append("name", document.get("worker_type"));
				String desc = "";
				if (document.containsKey("desc")
						&& document.get("desc") != null
						&& document.get("desc") instanceof String) {
					desc = document.getString("desc");
				}
				software.append("desc", desc);
				software.append("status", getDeviceStatus(document.get("ping_time")));
				if (document.containsKey("port")) {
					software.append("port", document.get("port"));
				}

				softwares.add(software);
			}
		}

		return deviceInfo;
	}

	private Document upperKeysInDocument(Document document) {
		Document retDoc = new Document();
		if (MapUtils.isNotEmpty(document)) {
			document.forEach((k, v) -> {
				if (v instanceof Document) {
					//Document temp = upperKeysInDocument((Document) v);
					retDoc.append(k.toUpperCase(), v);
				} else if (v instanceof List) {
                    /*List temp = new ArrayList();
                    ((List) v).forEach(sub -> {
                        if (sub instanceof Document) {
                            Document tempDoc = upperKeysInDocument((Document) sub);

                            temp.add(tempDoc);
                        } else {
                            temp.add(sub);
                        }
                    });*/

					retDoc.append(k.toUpperCase(), v);
				} else {
					retDoc.append(k.toUpperCase(), v);
				}
			});
		}

		return retDoc;
	}

	private void setCurrentTime() {
		Document serverStatus = context.getClientMongoOperator().getMongoTemplate().getDb().runCommand(new Document("serverStatus", 1));
		if (serverStatus.containsKey("localTime") && serverStatus.get("localTime") instanceof Date) {
			this.currentTime = new BigDecimal(serverStatus.getDate("localTime").getTime());
		}
	}
}
