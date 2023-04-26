package com.tapdata.processor.dataflow.aggregation.incr.service.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReturnDocument;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.ProcessorContext;
import com.tapdata.processor.dataflow.aggregation.incr.func.Func;
import com.tapdata.processor.dataflow.aggregation.incr.service.SnapshotService;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.AvgBucket;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.CountBucket;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.MaxBucket;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.MinBucket;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.SumBucket;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

public class MongoSnapshotServiceImpl extends AbstractMongoService implements SnapshotService<MongoSnapshotRecord> {

	private static final String DOC_ID = "_id";
	private static final String FORMAT_DOC_ID = "$_id";
	private final MongoCollection<Document> collection;

	public MongoSnapshotServiceImpl(Stage stage, ProcessorContext processorContext) {
		super(stage, processorContext.getTargetConn());
		String fid = processorContext.getJob().getDataFlowId().replaceAll("\\-", "");
		String sid = stage.getId().replaceAll("\\-", "");
		this.collection = this.database.getCollection(String.format("tapdata_snapshot_%s_%s", fid, sid));
	}

	@Override
	public MongoSnapshotRecord wrapRecord(List<String> primaryKeyFieldList, Map<String, Object> dataMap) {
		return new MongoSnapshotRecord(primaryKeyFieldList, dataMap);
	}

	@Override
	public MongoSnapshotRecord findOneAndReplace(MongoSnapshotRecord record) {
		final Bson eq = eq(DOC_ID, record.getRecordValue(DOC_ID));
		final Document oldDocument = this.collection.findOneAndReplace(eq, record.getRecord(), new FindOneAndReplaceOptions().upsert(true).returnDocument(ReturnDocument.BEFORE));
		return oldDocument != null ? new MongoSnapshotRecord(oldDocument) : null;
	}

	@Override
	public MongoSnapshotRecord findOneAndModify(MongoSnapshotRecord record) {
		final Document doc = record.getRecord();
		final Object id = doc.remove(DOC_ID);
		final MongoSnapshotRecord old = new MongoSnapshotRecord(this.collection.findOneAndUpdate(eq(DOC_ID, id), new BasicDBObject("$set", doc)));
		doc.put(DOC_ID, id);
		return old;
	}

	@Override
	public MongoSnapshotRecord findOneAndRemove(MongoSnapshotRecord record) {
		return new MongoSnapshotRecord(this.collection.findOneAndDelete(eq(DOC_ID, record.getRecordValue(DOC_ID))));
	}

	@Override
	public SumBucket sum(Map<String, Object> groupByMap, String valueField) {
		final Bson group = group(null, Arrays.asList(Accumulators.sum(Func.SUM.name(), formatField(valueField)), Accumulators.sum(Func.COUNT.name(), 1)));
		Document document = this.aggregate(groupByMap, group);
		if (document == null) {
			return new SumBucket(groupByMap, null, 0L);
		} else {
			return new SumBucket(groupByMap, document.get(Func.SUM.name(), Number.class), document.get(Func.COUNT.name(), Number.class).longValue());
		}
	}

	@Override
	public CountBucket count(Map<String, Object> groupByMap) {
		final Document document = this.aggregate(groupByMap, Aggregates.count(Func.COUNT.name()));
		if (document == null) {
			return new CountBucket(groupByMap, 0L);
		} else {
			return new CountBucket(groupByMap, document.get(Func.COUNT.name(), Number.class).longValue());
		}
	}

	@Override
	public MaxBucket max(Map<String, Object> groupByMap, String valueField) {
		final String MAX_ID = "MAX_ID";
		final Bson sort = sort(new BasicDBObject(valueField, -1));
		final Bson group = group(null, Arrays.asList(Accumulators.max(Func.MAX.name(), formatField(valueField)), Accumulators.sum(Func.COUNT.name(), 1), Accumulators.first(MAX_ID, FORMAT_DOC_ID)));
		Document document = this.aggregate(groupByMap, Arrays.asList(sort, group));
		if (document == null) {
			return new MaxBucket(groupByMap, null, null, 0L);
		} else {
			return new MaxBucket(groupByMap, document.get(MAX_ID), document.get(Func.MAX.name(), Number.class), document.get(Func.COUNT.name(), Number.class).longValue());
		}
	}

	@Override
	public MinBucket min(Map<String, Object> groupByMap, String valueField) {
		final String MIN_ID = "MIN_ID";
		final Bson sort = sort(new BasicDBObject(valueField, 1));
		final Bson group = group(null, Arrays.asList(Accumulators.min(Func.MIN.name(), formatField(valueField)), Accumulators.sum(Func.COUNT.name(), 1), Accumulators.first(MIN_ID, FORMAT_DOC_ID)));
		Document document = this.aggregate(groupByMap, Arrays.asList(sort, group));
		if (document == null) {
			return new MinBucket(groupByMap, null, null, 0L);
		} else {
			return new MinBucket(groupByMap, document.get(MIN_ID), document.get(Func.MIN.name(), Number.class), document.get(Func.COUNT.name(), Number.class).longValue());
		}
	}

	@Override
	public AvgBucket avg(Map<String, Object> groupByMap, String valueField) {
		final String formatValue = formatField(valueField);
		final Bson group = group(null, Arrays.asList(Accumulators.avg(Func.AVG.name(), formatValue), Accumulators.sum(Func.SUM.name(), formatValue), Accumulators.sum(Func.COUNT.name(), 1)));
		final Document document = this.aggregate(groupByMap, group);
		if (document != null) {
			return new AvgBucket(groupByMap, document.get(Func.AVG.name(), Number.class), document.get(Func.SUM.name(), Number.class), document.get(Func.COUNT.name(), Number.class).longValue());
		} else {
			return new AvgBucket(groupByMap, null, null, 0L);
		}
	}

	@Override
	public List<SumBucket> sumGroup(List<String> groupByFieldList, String valueField) {
		final Bson group = group(formatGrupByField(groupByFieldList), Arrays.asList(Accumulators.sum(Func.SUM.name(), formatField(valueField)), Accumulators.sum(Func.COUNT.name(), 1)));
		final AggregateIterable<Document> aggregateIterable = this.doAggregate(null, Collections.singletonList(group));
		List<SumBucket> sumBucketList = new ArrayList<>();
		aggregateIterable.forEach((Consumer<Document>) doc -> sumBucketList.add(new SumBucket(doc.get(DOC_ID, Map.class), doc.get(Func.SUM.name(), Number.class), doc.get(Func.COUNT.name(), Number.class).longValue())));
		return sumBucketList;
	}

	@Override
	public List<CountBucket> countGroup(List<String> groupByFieldList) {
		final Bson group = group(formatGrupByField(groupByFieldList), Accumulators.sum(Func.COUNT.name(), 1));
		final AggregateIterable<Document> aggregateIterable = this.doAggregate(null, Collections.singletonList(group));
		List<CountBucket> countBucketList = new ArrayList<>();
		aggregateIterable.forEach((Consumer<Document>) doc -> countBucketList.add(new CountBucket(doc.get(DOC_ID, Map.class), doc.get(Func.COUNT.name(), Number.class).longValue())));
		return countBucketList;
	}

	@Override
	public List<MaxBucket> maxGroup(List<String> groupByFieldList, String valueField) {
		final String MAX_ID = "MAX_ID";
		final Bson sort = sort(new BasicDBObject(valueField, -1));
		final Bson group = group(formatGrupByField(groupByFieldList), Arrays.asList(Accumulators.max(Func.MAX.name(), formatField(valueField)), Accumulators.sum(Func.COUNT.name(), 1), Accumulators.first(MAX_ID, FORMAT_DOC_ID)));
		final AggregateIterable<Document> aggregateIterable = this.doAggregate(null, Arrays.asList(sort, group));
		List<MaxBucket> maxBucketList = new ArrayList<>();
		aggregateIterable.forEach((Consumer<Document>) doc -> maxBucketList.add(new MaxBucket(doc.get(DOC_ID, Map.class), doc.get(MAX_ID), doc.get(Func.MAX.name(), Number.class), doc.get(Func.COUNT.name(), Number.class).longValue())));
		return maxBucketList;
	}

	@Override
	public List<MinBucket> minGroup(List<String> groupByFieldList, String valueField) {
		final String MIN_ID = "MIN_ID";
		final Bson sort = sort(new BasicDBObject(valueField, 1));
		final Bson group = group(formatGrupByField(groupByFieldList), Arrays.asList(Accumulators.min(Func.MIN.name(), formatField(valueField)), Accumulators.sum(Func.COUNT.name(), 1), Accumulators.first(MIN_ID, FORMAT_DOC_ID)));
		final AggregateIterable<Document> aggregateIterable = this.doAggregate(null, Arrays.asList(sort, group));
		List<MinBucket> minBucketList = new ArrayList<>();
		aggregateIterable.forEach((Consumer<Document>) doc -> minBucketList.add(new MinBucket(doc.get(DOC_ID, Map.class), doc.get(MIN_ID), doc.get(Func.MIN.name(), Number.class), doc.get(Func.COUNT.name(), Number.class).longValue())));
		return minBucketList;
	}

	@Override
	public List<AvgBucket> avgGroup(List<String> groupByFieldList, String valueField) {
		final String formatValue = formatField(valueField);
		final Bson group = group(formatGrupByField(groupByFieldList), Arrays.asList(Accumulators.avg(Func.AVG.name(), formatValue), Accumulators.sum(Func.SUM.name(), formatValue), Accumulators.sum(Func.COUNT.name(), 1)));
		final AggregateIterable<Document> aggregateIterable = this.doAggregate(null, Collections.singletonList(group));
		List<AvgBucket> avgBucketList = new ArrayList<>();
		aggregateIterable.forEach((Consumer<Document>) doc -> avgBucketList.add(new AvgBucket(doc.get(DOC_ID, Map.class), doc.get(Func.AVG.name(), Number.class), doc.get(Func.SUM.name(), Number.class), doc.get(Func.COUNT.name(), Number.class).longValue())));
		return avgBucketList;
	}

	@Override
	public void reset() {
		this.collection.drop();
	}

	@Override
	public String createIndex(List<String> keyFieldList) {
		return this.collection.createIndex(Indexes.ascending(keyFieldList), new IndexOptions().name(UUID.randomUUID().toString()).background(true));
	}

	private static String formatField(String field) {
		return String.format("$%s", field);
	}

	private static Bson formatGrupByField(List<String> groupByFieldList) {
		Document document = new Document();
		groupByFieldList.forEach(s -> document.append(s, formatField(s)));
		return new BasicDBObject(DOC_ID, document);
	}

	private Document aggregate(Map<String, Object> groupByMap, Bson bson) {
		return this.doAggregate(groupByMap, Collections.singletonList(bson)).first();
	}

	private Document aggregate(Map<String, Object> groupByMap, List<Bson> bsonList) {
		return this.doAggregate(groupByMap, bsonList).first();
	}

	private AggregateIterable<Document> doAggregate(Map<String, Object> groupByMap, List<Bson> bsonList) {
		final List<Bson> condition;
		if (groupByMap == null || groupByMap.isEmpty()) {
			condition = Collections.emptyList();
		} else {
			condition = groupByMap.entrySet().stream().map(e -> eq(e.getKey(), e.getValue())).collect(Collectors.toList());
		}
		final List<Bson> pipeline;
		if (condition.isEmpty()) {
			pipeline = bsonList;
		} else {
			pipeline = new ArrayList<>();
			pipeline.add(match(and(condition)));
			pipeline.addAll(bsonList);
		}
		return this.collection.aggregate(pipeline);
	}

}
