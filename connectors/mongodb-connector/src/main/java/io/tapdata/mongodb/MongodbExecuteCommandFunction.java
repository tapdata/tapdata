package io.tapdata.mongodb;

import com.mongodb.client.*;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.collections4.MapUtils;
import org.bson.Document;

import java.util.*;

public class MongodbExecuteCommandFunction {

  public long execute(Map<String, Object> executeObj, MongoClient mongoClient) {

    long resultRows;

    if (MapUtils.isNotEmpty(executeObj)) {

      ExecuteObject executeObject = new ExecuteObject(executeObj);
      String op = executeObject.getOp();
      if (op == null || "".equals(op)) {
        throw new RuntimeException("Mapping javascript process failed, op cannot be blank");
      }

      String database = executeObject.getDatabase();
      String collection = executeObject.getCollection();
      if (collection == null || "".equals(collection)) {
        throw new RuntimeException("Mapping javascript process failed, collection Name cannot be blank");
      }
      Map<String, Object> opObject = executeObject.getOpObject();
      Map<String, Object> filter = executeObject.getFilter();
      try {
        switch (op) {
          case ExecuteObject.INSERT_OP:

            if (MapUtils.isEmpty(opObject)) {
              throw new RuntimeException("Mapping javascript process failed, opObject cannot be empty for insert operation");
            }

            mongoClient.getDatabase(database).getCollection(collection).insertOne(new Document(opObject));
            resultRows = 1;
            break;
          case ExecuteObject.DELETE_OP:
            if (MapUtils.isEmpty(filter)) {
              throw new RuntimeException("Mapping javascript process failed, filter cannot be empty for delete operation");
            }

            DeleteResult deleteResult = mongoClient.getDatabase(database).getCollection(collection).deleteMany(new Document(filter));
            resultRows = deleteResult.getDeletedCount();
            break;
          case ExecuteObject.UPDATE_OP:

            if (MapUtils.isEmpty(opObject)) {
              throw new RuntimeException("Mapping javascript process failed, opObject cannot be empty for update operation");
            }

            if (MapUtils.isEmpty(filter)) {
              throw new RuntimeException("Mapping javascript process failed, filter cannot be empty for update operation");
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
              updateResult = mongoClient.getDatabase(database).getCollection(collection).updateMany(new Document(filter), updateDoc, options);
            } else {
              updateResult = mongoClient.getDatabase(database).getCollection(collection).updateOne(new Document(filter), updateDoc, options);
            }
            resultRows = updateResult.getModifiedCount();
            break;
          default:
            throw new RuntimeException(String.format("Mapping javascript process failed, unsupported this op %s", op));
        }
      } catch (Exception e) {
        throw new RuntimeException(String.format("Mapping javascript process for connection %s failed %s", "", e.getMessage()), e);
      }
    } else {
      throw new RuntimeException("Mapping javascript process failed, execute object can not be null");
    }

    return resultRows;
  }

  public List<Map<String, Object>> executeQuery(Map<String, Object> executeObj, MongoClient mongoClient) {

    if (MapUtils.isNotEmpty(executeObj)) {

      ExecuteObject executeObject = new ExecuteObject(Collections.unmodifiableMap(executeObj));
      return executeQuery(executeObject, mongoClient);
    } else {
      throw new RuntimeException(String.format("Mapping javascript process execute %s failed, execute object can not be null", executeObj));
    }

  }

  public List<Map<String, Object>> executeQuery(ExecuteObject executeObject, MongoClient mongoClient) {

    String database = executeObject.getDatabase();
    String collection = executeObject.getCollection();
    if (collection == null || "".equals(collection)) {
      throw new RuntimeException(String.format("Mapping javascript process execute %s failed, collection Name cannot be blank", executeObject));
    }
    Map<String, Object> filter = executeObject.getFilter();
    Document filterDocument = filter == null ? new Document() : new Document(filter);

    Map<String, Object> projection = executeObject.getProjection();
    Document projectionDocument = projection == null ? new Document() : new Document(projection);
    FindIterable<Document> findIterable = mongoClient.getDatabase(database).getCollection(collection).find(filterDocument).projection(projectionDocument);
    Map<String, Object> sort = executeObject.getSort();
    if (MapUtils.isNotEmpty(sort)) {
      Document sortDocument = new Document(sort);
      findIterable.sort(sortDocument);
    }

    int limit = executeObject.getLimit();
    if (limit > 0) {
      findIterable.limit(limit);
    }

    return getResultList(findIterable);
  }

  public long count(Map<String, Object> parameters, MongoClient mongoClient) {
    ExecuteObject executeObject = new ExecuteObject(parameters);
    String database = executeObject.getDatabase();
    String collection = executeObject.getCollection();
    if (collection == null || "".equals(collection)) {
      throw new RuntimeException(String.format("Mapping javascript process count %s failed, collection Name cannot be blank", executeObject));
    }
    Map<String, Object> filter = executeObject.getFilter();
    Document filterDocument = filter == null ? new Document() : new Document(filter);

    return mongoClient.getDatabase(database).getCollection(collection).countDocuments(filterDocument);
  }

  public Object aggregate(Map<String, Object> executeObj, MongoClient mongoClient) {
    ExecuteObject executeObject = new ExecuteObject(executeObj);
    String database = executeObject.getDatabase();
    String collection = executeObject.getCollection();
    if (collection == null || "".equals(collection)) {
      throw new RuntimeException(String.format("Mapping javascript process execute %s failed, collection Name cannot be blank", executeObject));
    }
    List<Map<String, Object>> pipeline = executeObject.getPipeline();
    List<Document> pipelines = new LinkedList<>();
    for (Map<String, Object> map : pipeline) {
      pipelines.add(new Document(map));
    }
    if (pipelines.size() == 0) {
      throw new RuntimeException(String.format("Mapping javascript process execute %s failed, pipeline cannot be blank", executeObject));
    }

    AggregateIterable<Document> aggregateIterable = mongoClient.getDatabase(database).getCollection(collection).aggregate(pipelines);
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
      throw new RuntimeException(String.format("process execute %s for connection failed %s", "iterable", e.getMessage()), e);
    }
  }
}
