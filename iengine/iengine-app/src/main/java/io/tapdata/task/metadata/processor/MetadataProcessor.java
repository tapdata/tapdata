package io.tapdata.task.metadata.processor;

import com.tapdata.mongo.ClientMongoOperator;
import org.bson.Document;

import java.util.List;

public interface MetadataProcessor {

	List<Document> process(String collectionName, Document record, String operationType, ClientMongoOperator clientMongoOperator);
}
