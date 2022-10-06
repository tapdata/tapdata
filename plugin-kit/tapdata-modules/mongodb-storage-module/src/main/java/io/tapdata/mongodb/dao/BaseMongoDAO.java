package io.tapdata.mongodb.dao;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.conversions.Bson;

import java.util.Collection;
import java.util.List;

public interface BaseMongoDAO<T> {
    boolean insertOne(T t);

    void insertAll(List<T> collections);

    DeleteResult deleteById(String id);

    DeleteResult deleteOne(Bson filter);

    DeleteResult deleteAll();

    DeleteResult delete(Bson filter);

    T findAndModify(Bson filter, Bson update);

    T findAndModify(Bson filter, Bson update, boolean upsert);

    T findAndModify(Bson filter, Bson update, FindOneAndUpdateOptions options);

    UpdateResult updateOne(Bson filter, Bson update);

    UpdateResult updateMany(Bson filter, Bson update);

    UpdateResult upsertOne(Bson filter, Bson update);

    UpdateResult upsertMany(Bson filter, Bson update);

    T findById(Object id);

    T findOne(Bson filter, String... fields);

    T findOne(Bson filter, Bson sort, String... fields);

    List<T> findByIdIn(List<String> ids);

    List<T> findByIdIn(Collection<String> ids);

    List<T> find(Bson filter, String... fields);

    List<T> find(Bson filter, int skip, int limit);

    List<T> find(Bson filter, Bson sort, int skip, int limit);

    List<T> findLimit(Bson filter, int limit);

    MongoCursor<T> findCursor(Bson filter, String... fields);
}