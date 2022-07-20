package com.tapdata.processor.dataflow.aggregation.incr.service.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.tapdata.entity.Mapping;
import com.tapdata.processor.dataflow.ProcessorContext;
import com.tapdata.processor.dataflow.aggregation.incr.service.AggregationService;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;

import static com.tapdata.entity.DataQualityTag.SUB_COLUMN_NAME;

public class MongoAggregationServiceImpl extends AbstractMongoService implements AggregationService {

	private MongoCollection<Document> collection;

	public MongoAggregationServiceImpl(ProcessorContext processorContext) {
		super(null, processorContext.getTargetConn());
		List<Mapping> mappingList = processorContext.getJob().getMappings();
		Mapping mapping = mappingList.get(mappingList.size() - 1);
		this.collection = this.database.getCollection(mapping.getTo_table());
	}

	@Override
	public long removeExpire(long version) {
		Bson filter;
		if (version == 1) {
			filter = Filters.eq(SUB_COLUMN_NAME + ".version", null);
		} else {
			filter = Filters.lt(SUB_COLUMN_NAME + ".version", version);
		}
		DeleteResult deleteResult = this.collection.deleteMany(filter);
		if (deleteResult.wasAcknowledged()) {
			return deleteResult.getDeletedCount();
		} else {
			return 0L;
		}
	}

}
