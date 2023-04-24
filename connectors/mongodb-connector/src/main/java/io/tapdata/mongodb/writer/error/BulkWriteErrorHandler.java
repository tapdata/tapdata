package io.tapdata.mongodb.writer.error;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import io.tapdata.mongodb.writer.BulkWriteModel;
import org.bson.Document;

/**
 * @author samuel
 * @Description
 * @create 2023-04-23 18:58
 **/
public interface BulkWriteErrorHandler {
	/**
	 * Handle mongodb bulk write error, the external will write again according to the returned write model and options
	 * Will match the corresponding BulkWriteErrorCodeHandler according to the code in the error{@link BulkWriteErrorCodeHandlerEnum},and call the handle method of the corresponding implementation class
	 *
	 * @param bulkWriteModel          Bulk write model{@link BulkWriteModel}
	 * @param writeModel              Error write models{@link WriteModel}
	 * @param bulkWriteOptions        Bulk write options{@link BulkWriteOptions}
	 * @param mongoBulkWriteException Bulk write exception{@link MongoBulkWriteException}
	 * @param writeError              Bulk write error{@link BulkWriteError}
	 * @return Retry write model{@link WriteModel}, if you cannot handle this error, return null
	 */
	WriteModel<Document> handle(
			BulkWriteModel bulkWriteModel,
			WriteModel<Document> writeModel,
			BulkWriteOptions bulkWriteOptions,
			MongoBulkWriteException mongoBulkWriteException,
			BulkWriteError writeError,
			MongoCollection<Document> collection
	);
}
