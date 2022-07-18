package com.tapdata.processor.dataflow.aggregation.incr.service.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.ProcessorContext;
import com.tapdata.processor.dataflow.aggregation.incr.service.SyncVersionService;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.mongodb.client.model.Filters.eq;

public class MongoSyncVersionServiceImpl extends AbstractMongoService implements SyncVersionService {

	private static final String TABLE_NAME = "tapdata_sync_version";
	private final MongoCollection<Document> collection;
	private final AtomicLong version;
	private final String id;

	public MongoSyncVersionServiceImpl(Stage stage, ProcessorContext processorContext) {
		super(stage, processorContext.getTargetConn());
		this.collection = this.database.getCollection(TABLE_NAME);

		String fid = processorContext.getJob().getDataFlowId().replaceAll("\\-", "");
		String sid = stage.getId().replaceAll("\\-", "");
		this.id = String.format("%s_%s", fid, sid);

		Document document = this.collection.find(eq("_id", this.id)).first();
		this.version = new AtomicLong(Optional.ofNullable(document).map(d -> d.get("version", Number.class).longValue()).orElse(0L));
	}

	@Override
	public long nextVersion() {
		Bson where = eq("_id", this.id);
		BasicDBObject incr = new BasicDBObject("$inc", new BasicDBObject("version", 1));
		Document document = this.collection.findOneAndUpdate(where, incr, new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
		long v = document.get("version", Number.class).longValue();
		version.set(v);
		return v;
	}

	@Override
	public long currentVersion() {
		return this.version.get();
	}


}
