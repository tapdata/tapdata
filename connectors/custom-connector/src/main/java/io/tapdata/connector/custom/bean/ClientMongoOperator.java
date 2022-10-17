package io.tapdata.connector.custom.bean;

import com.mongodb.*;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;
import io.tapdata.connector.custom.exception.BulkWriteErrorHandler;
import io.tapdata.constant.TapLog;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.kit.EmptyKit;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientMongoOperator {

    private static final String TAG = ClientMongoOperator.class.getSimpleName();

    private final MongoTemplate mongoTemplate;
    private final MongoClient mongoClient;
    private MongoClientURI mongoClientURI;
    private static final String MONGODB_DUPLICATE_ERROR_STRING = "E11000 duplicate key error";
    protected String cloudRegion;

    public ClientMongoOperator(MongoTemplate template, MongoClient mongoClient) {
        this.mongoTemplate = template;
        this.mongoClient = mongoClient;
    }

    public ClientMongoOperator(MongoTemplate mongoTemplate, MongoClient mongoClient, MongoClientURI mongoClientURI) {
        this.mongoTemplate = mongoTemplate;
        this.mongoClient = mongoClient;
        this.mongoClientURI = mongoClientURI;
    }

    public MongoTemplate getMongoTemplate() {
        return mongoTemplate;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public void upsert(Map<String, Object> params, Map<String, Object> insert, String collection) {
        Query query = new Query(getAndCriteria(params));
        Update update = getUpdate(insert);
        mongoTemplate.upsert(query, update, collection);
    }

    public <T> T upsert(Map<String, Object> params, Map<String, Object> insert, String collection, Class<T> clazz) {
        Query query = new Query(getAndCriteria(params));
        Update update = getUpdate(insert);
        mongoTemplate.upsert(query, update, collection);

        return mongoTemplate.findOne(new Query(getAndCriteria(params)), clazz);
    }

    public void delete(Map<String, Object> params, String collection) {
        Query query = new Query(getAndCriteria(params));
        mongoTemplate.remove(query, collection);
    }

    public BulkOperations getBulkOperations(String collection) {
        return mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, collection);
    }

    public void delete(Query query, String collection) {
        mongoTemplate.remove(query, collection);
    }

    public void deleteAll(Map<String, Object> params, String collection) {
        if (EmptyKit.isNotEmpty(params)) {
            mongoTemplate.remove(new Query(getAndCriteria(params)));
        }
    }

    public <T> T findAndModify(Map<String, Object> params, Map<String, Object> updateParams, Class<T> className, String collection) {
        Query query = new Query(getAndCriteria(params));
        Update update = getUpdate(updateParams);
        preModifyJobColl(update, collection);
        return mongoTemplate.findAndModify(query, update, new FindAndModifyOptions().returnNew(true), className, collection);
    }

    public <T> T findAndModify(Query query, Update update, Class<T> className, String collection) {
        preModifyJobColl(update, collection);
        return mongoTemplate.findAndModify(query, update, new FindAndModifyOptions().returnNew(true), className, collection);
    }

    public <T> T findAndModify(Query query, Update update, Class<T> className, String collection, boolean returnNew) {
        preModifyJobColl(update, collection);
        return mongoTemplate.findAndModify(query, update, new FindAndModifyOptions().returnNew(returnNew), className, collection);
    }


    public UpdateResult updateAndParam(Map<String, Object> params, Map<String, Object> updateParams, String collection) {
        Query query = new Query(getAndCriteria(params));
        Update update = getUpdate(updateParams);
        preModifyJobColl(update, collection);
        return mongoTemplate.updateFirst(query, update, collection);
    }

    public void updateOrParam(Map<String, Object> params, Map<String, Object> updateParams, String collection) {
        Query query = new Query(getAndCriteria(params));
        Update update = getUpdate(updateParams);

        mongoTemplate.updateMulti(query, update, collection);
    }

    public <T> List<T> find(Map<String, Object> params, String collection, Class<T> className) {
        Query query = new Query(getAndCriteria(params));
        return mongoTemplate.find(query, className, collection);
    }

    public <T> T findOne(Query query, String collection, Class<T> className) {
        return mongoTemplate.findOne(query, className, collection);
    }

    public <T> T findOne(Map<String, Object> params, String collection, Class<T> className) {
        Criteria criteria = new Criteria();
        params.forEach((k, v) -> criteria.and(k).is(v));
        Query query = new Query(criteria);
        return mongoTemplate.findOne(query, className, collection);
    }

    public UpdateResult update(Query query, Update update, String collection) {
        preModifyJobColl(update, collection);
        return mongoTemplate.updateFirst(query, update, collection);
    }

    public <T> List<T> find(Query query, String collection, Class<T> className) {
        return mongoTemplate.find(query, className, collection);
    }

    public TapBulkWriteResult executeBulkWrite(List<WriteModel<Document>> value,
                                               BulkWriteOptions options,
                                               String collectionName,
                                               int errorRetry,
                                               long retryInteval,
                                               AtomicBoolean running) throws Exception {
        MongoCollection<Document> collection = mongoTemplate.getCollection(collectionName);

        TapBulkWriteResult result = new TapBulkWriteResult();
        BulkWriteResult bulkWriteResult = BulkWriteResult.acknowledged(0, 0, 0, 0, new ArrayList<>());
        int retry = 0;
        while (EmptyKit.isNotEmpty(value)) {
            try {
                BulkWriteResult writeResult;
                if (options != null) {
                    writeResult = collection.bulkWrite(value, options);
                } else {
                    writeResult = collection.bulkWrite(value);
                }
                bulkWriteResult = statsBulkResult(bulkWriteResult, writeResult);
                break;
            } catch (MongoSocketException |
                     MongoNotPrimaryException |
                     MongoClientException |
                     MongoNodeIsRecoveringException |
                     MongoInterruptedException e) {

                retry = retryNetworkException(retryInteval, retry, e);

            } catch (MongoBulkWriteException e) {
                // 分片集切主时，写入数据失败进行重试（应用写入具有幂等性，value 不用过滤）
                boolean needRetry = false;
                if (running.get() && -3 == e.getCode()) {
                    BulkWriteError writeError;
                    List<BulkWriteError> writeErrors = e.getWriteErrors();
                    for (int i = 0, len = writeErrors.size(); i < len && !needRetry; i++) {
                        writeError = writeErrors.get(i);
                        needRetry = (
                                83 == writeError.getCode() // write results unavailable from localhost:59019 :: caused by :: Location17255: error receiving write command response, possible socket exception - see logs
                                        || 133 == writeError.getCode() // could not find host matching read preference { mode: "primary", tags: [ {} ] } for set shard02', details={ }
                                        || 10107 == writeError.getCode() // not master
                        );
                    }
                }

                if (needRetry) {
                    retry = retryNetworkException(retryInteval, retry, e);
                } else {
                    boolean errorHandleResult = handleBulkWriteError(e, value, options);
                    if (!errorHandleResult) {
                        value = continueExecuteBulkWrite(collectionName, value, options, e);
                        // stats all bulk result
                        BulkWriteResult writeResult = e.getWriteResult();
                        bulkWriteResult = statsBulkResult(bulkWriteResult, writeResult);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(
                        String.format(
                                "Bulk write to collection %s failed %s, %s",
                                collectionName,
                                e.getMessage(),
                                EmptyKit.isNotEmpty(value) ? value.get(0) : null
                        ),
                        e
                );
            }
        }

        result.setBulkWriteResult(bulkWriteResult);

        return result;
    }

    private BulkWriteResult statsBulkResult(BulkWriteResult totalWriteResult, BulkWriteResult writeResult) {
        if (totalWriteResult == null || !totalWriteResult.wasAcknowledged()) {
            totalWriteResult = writeResult;
        } else {
            int deletedCount = writeResult.getDeletedCount() + totalWriteResult.getDeletedCount();
            int insertedCount = writeResult.getInsertedCount() + totalWriteResult.getInsertedCount();
            int modifiedCount = writeResult.getModifiedCount() + totalWriteResult.getModifiedCount();
            int matchedCount = writeResult.getMatchedCount() + totalWriteResult.getMatchedCount();
            List<BulkWriteUpsert> bulkWriteUpserts = totalWriteResult.getUpserts() == null ? new ArrayList<>() : totalWriteResult.getUpserts();
            if (EmptyKit.isNotEmpty(writeResult.getUpserts())) {
                bulkWriteUpserts.addAll(writeResult.getUpserts());
            }
            totalWriteResult = BulkWriteResult.acknowledged(insertedCount, matchedCount, deletedCount, modifiedCount, bulkWriteUpserts);
        }
        return totalWriteResult;
    }

    private List<WriteModel<Document>> continueExecuteBulkWrite(String collectionName, List<WriteModel<Document>> value, BulkWriteOptions options, MongoBulkWriteException e) {

        List<BulkWriteError> bulkWriteErrors = e.getWriteErrors();

        if (EmptyKit.isNotEmpty(value) && EmptyKit.isNotEmpty(bulkWriteErrors)) {

            int size = value.size();
            int errorSize = bulkWriteErrors.size();

            if ((options == null || options.isOrdered()) && errorSize < size) {
                if (EmptyKit.isNotEmpty(bulkWriteErrors)) {
                    BulkWriteError bulkWriteError = bulkWriteErrors.get(0);
                    int index = bulkWriteError.getIndex();
                    WriteModel<Document> errorWrite = value.get(index);
                    value = index + 1 < size ? value.subList(index + 1, size) : null;
                    TapLogger.warn(TAG,
                            TapLog.TRAN_ERROR_0028.getMsg(),
                            collectionName,
                            e.getMessage(),
                            size,
                            bulkWriteErrors.size(),
                            value == null ? 0 : value.size(),
                            errorWrite
                    );
                } else {
                    TapLogger.warn(TAG, TapLog.TRAN_ERROR_0014.getMsg(), collectionName, e.getMessage());
                }

            } else {
                String message = e.getMessage();
                String s = message.length() > 500 ? message.substring(0, 500) : message;
                if (StringUtils.containsIgnoreCase(message, MONGODB_DUPLICATE_ERROR_STRING)) {
                    TapLogger.warn(TAG, TapLog.W_TRAN_LOG_0004.getMsg(), collectionName, size, size - errorSize, errorSize, s, e);
                } else {
                    TapLogger.error(TAG, TapLog.TRAN_ERROR_0013.getMsg(), collectionName, s, size, size - errorSize, errorSize, e);
                }
                value = null;
            }
        } else {
            if (options != null && !options.isOrdered()) {
                value = null;
            }
        }
        return value;
    }

    private int retryNetworkException(long retryInteval, int retry, MongoException e) {
        retry++;

//        if (retry > errorRetry) {
//            throw new Exception(e);
//        }
        TapLogger.warn(TAG, TapLog.TRAN_ERROR_0012.getMsg(), e.getMessage(), retryInteval, e);
        try {
            Thread.sleep(retryInteval);
        } catch (InterruptedException e1) {
            //abort
        }
        return retry;
    }

    public long postCount(Query query, String collection) {
        return count(query, collection);
    }

    public long count(Query query, String collection) {
        return mongoTemplate.count(query, collection);
    }

    public void pullObjectToArray(Map<String, Object> params, Map<String, Object> updateParams, String collection) {
        Query query = new Query(getAndCriteria(params));
        Update update = new Update();
        updateParams.forEach(update::push);
        preModifyJobColl(update, collection);
        mongoTemplate.updateFirst(query, update, collection);

    }

    private Criteria getAndCriteria(Map<String, Object> queryParam) {
        Criteria criteria = new Criteria();
        if (queryParam != null && !queryParam.isEmpty()) {
            queryParam.forEach((s, o) -> criteria.and(s).is(o));
        }
        return criteria;
    }

    private Criteria getOrCriteria(Map<String, Object> queryParam) {
        Criteria criteria = new Criteria();
        if (queryParam != null && !queryParam.isEmpty()) {
            queryParam.forEach((s, o) -> criteria.in(o));
        }
        return criteria;
    }

    public void createIndexes(String collection, List<IndexModel> indexModels) {
        MongoCollection<Document> mongoCollection = mongoTemplate.getCollection(collection);
        mongoCollection.createIndexes(indexModels);
    }

    public void createIndex(String collection, Bson keys, IndexOptions options) {
        MongoCollection<Document> mongoCollection = mongoTemplate.getCollection(collection);
        mongoCollection.createIndex(keys, options);
    }

    public boolean existIndex(String collection, String indexName) {
        MongoCollection<Document> mongoCollection = mongoTemplate.getCollection(collection);
        for (Document doc : mongoCollection.listIndexes()) {
            if (indexName.equalsIgnoreCase(doc.getString("name"))) {
                return true;
            }
        }
        return false;
    }

    public void insertOne(Object obj, String collection) {
        mongoTemplate.insert(obj, collection);
    }

    public void insertList(List<?> list, String collection) {
        mongoTemplate.insert(list, collection);
    }

    public void dropCollection(String collection) {
        mongoTemplate.dropCollection(collection);
    }

    public Set<String> getCollectionNames() {
        return mongoTemplate.getCollectionNames();
    }

    private Update getUpdate(Map<String, Object> insert) {
        Update update = new Update();
        if (insert != null && !insert.isEmpty()) {
            insert.forEach(update::set);
        }
        return update;
    }

    public void releaseResource() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    private Update preModifyJobColl(Update update, String collection) {
        if ("Jobs".equals(collection)) {
            update.set("last_update", System.currentTimeMillis());
        }
        return update;
    }

    public MongoClientURI getMongoClientURI() {
        return mongoClientURI;
    }

    public void setCloudRegion(String cloudRegion) {
        this.cloudRegion = cloudRegion;
    }

    private boolean handleBulkWriteError(MongoBulkWriteException e, List<WriteModel<Document>> writeModels, BulkWriteOptions bulkWriteOptions) throws Exception {
        List<BulkWriteError> writeErrors = e.getWriteErrors();

        for (BulkWriteError writeError : writeErrors) {
            BulkWriteErrorRetrievableCode bulkWriteErrorRetrievableCode = BulkWriteErrorRetrievableCode.fromCode(writeError.getCode());
            if (bulkWriteErrorRetrievableCode == null) {
                return false;
            }
            String implementClass = bulkWriteErrorRetrievableCode.getImplementClass();
            Class<?> errorHandlerClazz = Class.forName(implementClass);
            Object errorHandler = errorHandlerClazz.newInstance();
            if (!(errorHandler instanceof BulkWriteErrorHandler)) {
                return false;
            }
            boolean result = ((BulkWriteErrorHandler) errorHandler).handle(writeModels.get(writeError.getIndex()), writeError);
            if (!result) {
                return false;
            }
        }

        return true;
    }

    enum BulkWriteErrorRetrievableCode {
        ERROR_28(28, "Cannot create field '.*' in element \\{.*: null\\}", "com.tapdata.mongo.error.handler.Code28Handler"),
        ;
        private final int code;
        private final String errorMessageRegex;
        private final String implementClass;

        private static final Map<Integer, BulkWriteErrorRetrievableCode> codeMap = new HashMap<>();

        static {
            for (BulkWriteErrorRetrievableCode value : BulkWriteErrorRetrievableCode.values()) {
                codeMap.put(value.getCode(), value);
            }
        }

        BulkWriteErrorRetrievableCode(int code, String errorMessageRegex, String implementClass) {
            this.code = code;
            this.errorMessageRegex = errorMessageRegex;
            this.implementClass = implementClass;
        }

        public static BulkWriteErrorRetrievableCode fromCode(int code) {
            return codeMap.get(code);
        }

        public int getCode() {
            return code;
        }

        public String getErrorMessageRegex() {
            return errorMessageRegex;
        }

        public String getImplementClass() {
            return implementClass;
        }
    }

    public GridFSBucket getGridFSBucket() {
        MongoDatabase mongoDatabase = mongoTemplate.getDb();
        return GridFSBuckets.create(mongoDatabase);
    }
}
