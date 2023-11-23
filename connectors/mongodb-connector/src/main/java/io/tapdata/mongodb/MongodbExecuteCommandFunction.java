package io.tapdata.mongodb;

import com.mongodb.client.*;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import io.tapdata.entity.simplify.TapSimplify;
import org.apache.commons.collections4.MapUtils;
import org.bson.Document;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class MongodbExecuteCommandFunction {

  public long execute(Map<String, Object> executeObj, MongoClient mongoClient) {

    long resultRows;

    if (MapUtils.isNotEmpty(executeObj)) {

      ExecuteObject executeObject = new ExecuteObject(executeObj);
      String op = executeObject.getOp();
      if (op == null || "".equals(op)) {
        throw new RuntimeException("Process failed, op cannot be blank");
      }

      String database = executeObject.getDatabase();
      String collection = executeObject.getCollection();
      if (collection == null || "".equals(collection)) {
        throw new RuntimeException("Process failed, collection Name cannot be blank");
      }
      Map<String, Object> opObject = executeObject.getOpObject();
      Document filter = executeObject.getFilter();
      try {
        switch (op) {
          case ExecuteObject.INSERT_OP:

            if (MapUtils.isEmpty(opObject)) {
              throw new RuntimeException("Process failed, opObject cannot be empty for insert operation");
            }

            mongoClient.getDatabase(database).getCollection(collection).insertOne(new Document(opObject));
            resultRows = 1;
            break;
          case ExecuteObject.DELETE_OP:
            if (null == filter || filter.isEmpty()) {
              throw new RuntimeException("Process failed, filter cannot be empty for delete operation");
            }

            DeleteResult deleteResult = mongoClient.getDatabase(database).getCollection(collection).deleteMany(filter);
            resultRows = deleteResult.getDeletedCount();
            break;
          case ExecuteObject.UPDATE_OP:

            if (MapUtils.isEmpty(opObject)) {
              throw new RuntimeException("Process failed, opObject cannot be empty for update operation");
            }

            if (null == filter || filter.isEmpty()) {
              throw new RuntimeException("Process failed, filter cannot be empty for update operation");
            }
            UpdateResult updateResult;
            UpdateOptions options = new UpdateOptions().upsert(executeObject.isUpsert());

            final Document updateDoc = new Document();
            if (opObject.keySet().stream().anyMatch(u -> u.startsWith("$"))) {
              updateDoc.putAll(opObject);
            } else {
              updateDoc.append("$set", new Document(opObject));
            }
            if (executeObject.isMulti()) {
              updateResult = mongoClient.getDatabase(database).getCollection(collection).updateMany(filter, updateDoc, options);
            } else {
              updateResult = mongoClient.getDatabase(database).getCollection(collection).updateOne(filter, updateDoc, options);
            }
            resultRows = updateResult.getModifiedCount();
            break;
          default:
            throw new RuntimeException(String.format("Process failed, unsupported this op %s", op));
        }
      } catch (Exception e) {
        throw new RuntimeException(String.format("Process for connection %s failed %s", "", e.getMessage()), e);
      }
    } else {
      throw new RuntimeException("Process failed, execute object can not be null");
    }

    return resultRows;
  }

  public void executeQuery(Map<String, Object> executeObj, MongoClient mongoClient, Consumer<List<Map<String, Object>>> consumer, Supplier<Boolean> aliveSupplier) {

    if (MapUtils.isNotEmpty(executeObj)) {

      ExecuteObject executeObject = new ExecuteObject(Collections.unmodifiableMap(executeObj));
      MongoIterable<Document> mongoIterable = getMongoIterable(executeObject, mongoClient);
      consumer(mongoIterable, consumer, executeObject.getBatchSize(), aliveSupplier);
    } else {
      throw new RuntimeException(String.format("Process execute %s failed, execute object can not be null", executeObj));
    }
  }

  public List<Map<String, Object>> executeQuery(Map<String, Object> executeObj, MongoClient mongoClient) {

    if (MapUtils.isNotEmpty(executeObj)) {
      ExecuteObject executeObject = new ExecuteObject(Collections.unmodifiableMap(executeObj));
      MongoIterable<Document> mongoIterable = getMongoIterable(executeObject, mongoClient);
      return getResultList(mongoIterable);

    } else {
      throw new RuntimeException(String.format("Process execute %s failed, execute object can not be null", executeObj));
    }

  }

  public MongoIterable<Document> getMongoIterable(ExecuteObject executeObject, MongoClient mongoClient) {

    String database = executeObject.getDatabase();
    String collection = executeObject.getCollection();
    if (collection == null || "".equals(collection)) {
      throw new RuntimeException(String.format("Process execute %s failed, collection Name cannot be blank", executeObject));
    }
    Document filter = executeObject.getFilter();

    Map<String, Object> projection = executeObject.getProjection();
    Document projectionDocument = projection == null ? new Document() : new Document(projection);
    FindIterable<Document> findIterable = mongoClient.getDatabase(database).getCollection(collection).find(filter).projection(projectionDocument);
    Map<String, Object> sort = executeObject.getSort();
    if (MapUtils.isNotEmpty(sort)) {
      Document sortDocument = new Document(sort);
      findIterable.sort(sortDocument);
    }

    int limit = executeObject.getLimit();
    if (limit > 0) {
      findIterable.limit(limit);
    }

    return findIterable;
  }

  public long count(Map<String, Object> parameters, MongoClient mongoClient) {
    ExecuteObject executeObject = new ExecuteObject(parameters);
    String database = executeObject.getDatabase();
    String collection = executeObject.getCollection();
    if (collection == null || "".equals(collection)) {
      throw new RuntimeException(String.format("Process count %s failed, collection Name cannot be blank", executeObject));
    }
    Document filter = executeObject.getFilter();
    return mongoClient.getDatabase(database).getCollection(collection).countDocuments(filter);
  }

  public void aggregate(Map<String, Object> executeObj, MongoClient mongoClient, Consumer<List<Map<String, Object>>> consumer, Supplier<Boolean> aliveSupplier) {
    AggregateIterable<Document> aggregateIterable = getAggregateIterable(executeObj, mongoClient);
    int batchSize = executeObj.get("batchSize") != null ? (int) executeObj.get("batchSize") : 1000;
    consumer(aggregateIterable, consumer, batchSize, aliveSupplier);
  }

  private static AggregateIterable<Document> getAggregateIterable(Map<String, Object> executeObj, MongoClient mongoClient) {
    ExecuteObject executeObject = new ExecuteObject(executeObj);
    String database = executeObject.getDatabase();
    String collection = executeObject.getCollection();
    if (collection == null || "".equals(collection)) {
      throw new RuntimeException(String.format("Process execute %s failed, collection Name cannot be blank", executeObject));
    }
    List<Document> pipelines = executeObject.getPipeline();
    if (pipelines.size() == 0) {
      throw new RuntimeException(String.format("Process execute %s failed, pipeline cannot be blank", executeObject));
    }

    return mongoClient.getDatabase(database).getCollection(collection).aggregate(pipelines).allowDiskUse(true);
  }

  public Object aggregate(Map<String, Object> executeObj, MongoClient mongoClient) {
    AggregateIterable<Document> aggregateIterable = getAggregateIterable(executeObj, mongoClient);
    return getResultList(aggregateIterable);
  }

  private List<Map<String, Object>> getResultList(MongoIterable<Document> mongoIterable) {
    try (MongoCursor<Document> mongoCursor = mongoIterable.iterator()) {

      List<Map<String, Object>> resultList = new ArrayList<>();
      while (mongoCursor.hasNext()) {
        resultList.add(mongoCursor.next());
      }

      return resultList;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Process execute %s for connection failed %s", "iterable", e.getMessage()), e);
    }
  }

  private void consumer(MongoIterable<Document> mongoIterable, Consumer<List<Map<String, Object>>> consumer, int batchSize, Supplier<Boolean> aliveSupplier) {
    try (MongoCursor<Document> mongoCursor = mongoIterable.iterator()) {

      List<Map<String, Object>> resultList = TapSimplify.list();
      while (mongoCursor.hasNext()) {
        if (!aliveSupplier.get()) {
          return;
        }
        resultList.add(mongoCursor.next());
        if (resultList.size() >= batchSize) {
          consumer.accept(resultList);
          resultList = TapSimplify.list();
        }
      }
      if (resultList.size() > 0) {
        consumer.accept(resultList);
      }
    } catch (Exception e) {
      throw new RuntimeException(String.format("Process execute %s for connection failed %s", "iterable", e.getMessage()), e);
    }
  }
}
