package io.tapdata.mongodb.writer;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.mongodb.MongodbUtil;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.mongodb.reader.MongodbV4StreamReader;
import io.tapdata.mongodb.util.MongodbLookupUtil;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.writeListResult;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * @author jackin
 * @date 2022/5/17 18:30
 **/
public class MongodbWriter {

		public static final String TAG = MongodbV4StreamReader.class.getSimpleName();

		protected MongoClient mongoClient;
		private MongoDatabase mongoDatabase;
		private KVMap<Object> globalStateMap;
		private ConnectionString connectionString;

		public MongodbWriter(KVMap<Object> globalStateMap) {
				this.globalStateMap = globalStateMap;
		}

		public void onStart(MongodbConfig mongodbConfig) {
				if (mongoClient == null) {
						mongoClient = MongodbUtil.createMongoClient(mongodbConfig);
						mongoDatabase = mongoClient.getDatabase(mongodbConfig.getDatabase());
						this.connectionString = new ConnectionString(mongodbConfig.getUri());
				}
		}

		/**
		 * The method invocation life circle is below,
		 * initiated ->
		 *  if(needCreateTable)
		 *      createTable
		 *  if(needClearTable)
		 *      clearTable
		 *  if(needDropTable)
		 *      dropTable
		 *  writeRecord
		 * -> destroy -> ended
		 *
		 * @param tapRecordEvents
		 * @param writeListResultConsumer
		 */
		public void writeRecord(List<TapRecordEvent> tapRecordEvents, TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
				AtomicLong inserted = new AtomicLong(0); //insert count
				AtomicLong updated = new AtomicLong(0); //update count
				AtomicLong deleted = new AtomicLong(0); //delete count

				List<WriteModel<Document>> writeModels = new ArrayList<>();
//				Map<String, List<Document>> insertMap = new HashMap<>();
//				Map<String, List<TapRecordEvent>> insertEventMap = new HashMap<>();
				UpdateOptions options = new UpdateOptions().upsert(true);

				WriteListResult<TapRecordEvent> writeListResult = writeListResult();

				MongoCollection<Document> collection = getMongoCollection(table.getId());

				final Collection<String> pks = table.primaryKeys(true);

				MongodbLookupUtil.lookUpAndSaveDeleteMessage(tapRecordEvents, this.globalStateMap, this.connectionString, pks, collection);

				for (TapRecordEvent recordEvent : tapRecordEvents) {
						final Map<String, Object> info = recordEvent.getInfo();
						if (MapUtils.isNotEmpty(info) && info.containsKey(MergeInfo.EVENT_INFO_KEY)) {
								final List<WriteModel<Document>> mergeWriteModels = MongodbMergeOperate.merge(inserted, updated, deleted, recordEvent, table);
								if (CollectionUtils.isNotEmpty(mergeWriteModels)) {
										writeModels.addAll(mergeWriteModels);
								}
						} else {
								WriteModel<Document> writeModel = normalWriteMode(inserted, updated, deleted, options, collection, pks, recordEvent);
								if (writeModel != null) {
										writeModels.add(writeModel);
								}
						}
				}

				if (CollectionUtils.isNotEmpty(writeModels)) {
						final MongoCollection<Document> mongoCollection = getMongoCollection(table.getId());
						mongoCollection.bulkWrite(writeModels, new BulkWriteOptions().ordered(true));
				}
				//Need to tell incremental engine the write result
				writeListResultConsumer.accept(writeListResult
								.insertedCount(inserted.get())
								.modifiedCount(updated.get())
								.removedCount(deleted.get()));
		}

		private WriteModel<Document> normalWriteMode(AtomicLong inserted, AtomicLong updated, AtomicLong deleted, UpdateOptions options, MongoCollection<Document> collection, Collection<String> pks, TapRecordEvent recordEvent) {
				WriteModel<Document> writeModel = null;
				if (recordEvent instanceof TapInsertRecordEvent) {
						TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;

						if (CollectionUtils.isNotEmpty(pks)) {
								final Document pkFilter = getPkFilter(pks, insertRecordEvent.getAfter());
//								writeModels.add(new InsertOneModel<>(new Document(insertRecordEvent.getAfter())));
								writeModel = new UpdateManyModel<>(pkFilter, new Document().append("$set", insertRecordEvent.getAfter()), options);
						} else {
								writeModel = new InsertOneModel<>(new Document(insertRecordEvent.getAfter()));
						}
						inserted.incrementAndGet();
				} else if (recordEvent instanceof TapUpdateRecordEvent && CollectionUtils.isNotEmpty(pks)) {

						TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
						Map<String, Object> after = updateRecordEvent.getAfter();
						Map<String, Object> before = updateRecordEvent.getBefore();
						final Document pkFilter = getPkFilter(pks, before != null && !before.isEmpty() ? before : after);

						writeModel = new UpdateManyModel<>(pkFilter, new Document().append("$set", after), options);
						updated.incrementAndGet();
				} else if (recordEvent instanceof TapDeleteRecordEvent && CollectionUtils.isNotEmpty(pks)) {

						TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
						Map<String, Object> before = deleteRecordEvent.getBefore();
						final Document pkFilter = getPkFilter(pks, before);

						writeModel = new DeleteOneModel<>(pkFilter);
						collection.deleteOne(new Document(before));
						deleted.incrementAndGet();
				}

				return writeModel;
		}

		public void onDestroy(){
				if (mongoClient != null) {
						mongoClient.close();
				}
		}

		private MongoCollection<Document> getMongoCollection(String table) {
				return mongoDatabase.getCollection(table);
		}

		private Document getPkFilter(Collection<String> pks, Map<String, Object> record) {
				Document filter = new Document();
				for (String pk : pks) {
						filter.append(pk, record.get(pk));
				}

				return filter;
		}
}
