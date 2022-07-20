package io.tapdata.task.metadata.processor;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;
import java.util.List;

public class GridFSMetadataProcessor implements MetadataProcessor {
	@Override
	public List<Document> process(String collectionName, Document record, String operationType, ClientMongoOperator clientMongoOperator) {

		List<Document> metadataRecords = new ArrayList<>();

		if ("Connections".equals(collectionName)) {
			MongoTemplate mongoTemplate = clientMongoOperator.getMongoTemplate();
			for (String collection : mongoTemplate.getDb().listCollectionNames()) {
				if (collectionName.endsWith(".files")) {
					for (Document file : mongoTemplate.getDb().getCollection(collection).find()) {
						file.put("collection_name", collection);
						String qualifiedName = file.getObjectId("_id").toHexString();

						Document source = new Document();
						source.putAll(file);
						source.put("_id", file.getObjectId("_id").toHexString());
						metadataRecords.add(new Document()
								.append("meta_type", file)
								.append("original_name", file.getString("filename"))
								.append("qualified_name", qualifiedName)
								.append("source", source)

						);
					}
				}
			}

			for (Document metadata : mongoTemplate.getDb().getCollection(ConnectorConstant.METADATA_INSTANCE_COLLECTION).find(
					new Document("meta_type", "file")
			).projection(new Document("meta_type", 1).append("source", 1).append("_id", 1))) {
				Document source = metadata.get("source", Document.class);
				if (MapUtils.isNotEmpty(source)) {
					String collection = source.getString("collection_name");
					if (StringUtils.isNotBlank(collection)) {

						Object sourceId = source.get("_id");
						if (sourceId instanceof String) {
							sourceId = new ObjectId((String) sourceId);
						}

						long fileCount = mongoTemplate.getDb().getCollection(collection).countDocuments(
								new Document("_id", sourceId)
						);
						if (fileCount <= 0) {
							mongoTemplate.getDb().getCollection(ConnectorConstant.METADATA_INSTANCE_COLLECTION).updateOne(
									new Document("_id", metadata.getObjectId("_id")),
									new Document("$set", new Document("is_deleted", true))
											.append("$unset", new Document()
													.append("lienage", 1)
													.append("fields_lienage", 1)
													.append("fields", 1)
													.append("indexes", 1)
											)
							);
						}
					}
				}
			}
		}
		return metadataRecords;
	}
}
