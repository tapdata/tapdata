package io.tapdata.mongodb.dao;

import com.google.common.collect.Lists;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import io.tapdata.entity.logger.TapLogger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.Collection;
import java.util.List;

public abstract class AbstractMongoDAO<T> implements BaseMongoDAO<T> {

    public static final String TAG = AbstractMongoDAO.class.getSimpleName();

    MongoCollection<T> mongoCollection;
    /**
     * 表名
     */
    String collectionName;

    @Override
    public boolean insertOne(T t) {
        InsertOneResult result = mongoCollection.insertOne(t);
        return result.getInsertedId() != null;
    }

    @Override
    public void insertAll(List<T> collections) {
        mongoCollection.insertMany(collections);
    }

    @Override
    public DeleteResult deleteById(String id) {
        return mongoCollection.deleteOne(Filters.eq(new ObjectId(id)));
    }

    @Override
    public DeleteResult deleteOne(Bson filter) {
        return mongoCollection.deleteOne(filter);
    }

    @Override
    public DeleteResult deleteAll() {
        return mongoCollection.deleteMany(new Document());
    }

    @Override
    public DeleteResult delete(Bson filter) {
        return mongoCollection.deleteMany(filter);
    }

    @Override
    public T findAndModify(Bson filter, Bson update) {
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        options.upsert(false);
        options.returnDocument(ReturnDocument.AFTER);
        return findAndModify(filter, update, options);
    }

    @Override
    public T findAndModify(Bson filter, Bson update, boolean upsert) {
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        options.upsert(upsert);
        options.returnDocument(ReturnDocument.AFTER);
        return findAndModify(filter, update, options);
    }

    @Override
    public T findAndModify(Bson filter, Bson update, FindOneAndUpdateOptions options) {
        return mongoCollection.findOneAndUpdate(filter, update, options);
    }

    @Override
    public UpdateResult updateOne(Bson filter, Bson update) {
        return updateOne(filter, update, false);
    }

    @Override
    public UpdateResult upsertOne(Bson filter, Bson update) {
        return updateOne(filter, update, true);
    }

    public UpdateResult updateOne(Bson filter, Bson update, boolean upsert) {
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(upsert);
        return mongoCollection.updateOne(filter, update, updateOptions);
    }

    @Override
    public UpdateResult updateMany(Bson filter, Bson update) {
        return updateMany(filter, update, false);
    }

    @Override
    public UpdateResult upsertMany(Bson filter, Bson update) {
        return updateMany(filter, update, true);
    }

    public UpdateResult updateMany(Bson filter, Bson update, boolean upsert) {
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(upsert);
        return mongoCollection.updateMany(filter, update, updateOptions);
    }

    @Override
    public T findById(Object id) {
        return mongoCollection.find(Filters.eq(id)).first();
    }

    @Override
    public T findOne(Bson filter, String... fields) {
        FindIterable<T> findIterable = mongoCollection.find(filter);
        if (isEmpty(fields)) {
            return findIterable.first();
        } else {
            Bson projection = Projections.include(fields);
            return findIterable.projection(projection).first();
        }
    }

    @Override
    public T findOne(Bson filter, Bson sort, String... fields) {
        FindIterable<T> findIterable = mongoCollection.find(filter);
        findIterable.limit(1);
        if (sort != null) {
            findIterable.sort(sort);
        }
        if (isEmpty(fields)) {
            return findIterable.first();
        } else {
            Bson projection = Projections.include(fields);
            return findIterable.projection(projection).first();
        }
    }

    public List<T> findByIdIn(List<String> ids) {
        Bson filter = Filters.in("_id", ids);
        return find(filter);
    }

    public List<T> findByIdIn(Collection<String> ids) {
        Bson filter = Filters.in("_id", ids);
        return find(filter);
    }

    @Override
    public List<T> find(Bson filter, String... fields) {
        MongoCursor<T> cursor = null;
        try {
            FindIterable<T> iterable = mongoCollection.find(filter);
            if (!isEmpty(fields)) {
                iterable.projection(Projections.include(fields));
            }
            cursor = iterable.cursor();
            List<T> list = Lists.newArrayList();
            while (cursor.hasNext()) {
                list.add(cursor.next());
            }
            return list;
        } finally {
            if (cursor != null) {
                try {
                    cursor.close();
                } catch (Exception e) {
                    TapLogger.error(TAG, "cursor close error:$e");
                }
            }
        }
    }

    @Override
    public List<T> find(Bson filter, int skip, int limit) {
        MongoCursor<T> cursor = null;
        try {
            FindIterable<T> iterable = mongoCollection.find(filter).skip(skip).limit(limit);
            cursor = iterable.cursor();
            List<T> list = Lists.newArrayList();
            while (cursor.hasNext()) {
                list.add(cursor.next());
            }
            return list;
        } finally {
            if (cursor != null) {
                try {
                    cursor.close();
                } catch (Exception e) {
                    TapLogger.error(TAG, "cursor close error:$e");
                }
            }
        }
    }

    @Override
    public List<T> find(Bson filter, Bson sort, int skip, int limit) {
        MongoCursor<T> cursor = null;
        try {
            FindIterable<T> iterable = mongoCollection.find(filter);
            if (sort != null) {
                iterable = iterable.sort(sort);
            }
            iterable = iterable.skip(skip).limit(limit);
            cursor = iterable.cursor();
            List<T> list = Lists.newArrayList();
            while (cursor.hasNext()) {
                list.add(cursor.next());
            }
            return list;
        } finally {
            if (cursor != null) {
                try {
                    cursor.close();
                } catch (Exception e) {
                    TapLogger.error(TAG, "cursor close error:$e");
                }
            }
        }
    }

    @Override
    public List<T> findLimit(Bson filter, int limit) {
        MongoCursor<T> cursor = null;
        try {
            FindIterable<T> iterable = mongoCollection.find(filter).limit(limit);
            cursor = iterable.cursor();
            List<T> list = Lists.newArrayList();
            while (cursor.hasNext()) {
                list.add(cursor.next());
            }
            return list;
        } finally {
            if (cursor != null) {
                try {
                    cursor.close();
                } catch (Exception e) {
                    TapLogger.error(TAG, "cursor close error:$e");
                }
            }
        }
    }

    @Override
    public MongoCursor<T> findCursor(Bson filter, String... fields) {
        FindIterable<T> iterable = mongoCollection.find(filter);
        if (!isEmpty(fields)) {
            iterable.projection(Projections.include(fields));
        }
        return iterable.cursor();
    }

    public boolean isEmpty(String... fields) {
        return fields == null || fields.length <= 0;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public MongoCollection<T> getMongoCollection() {
        return mongoCollection;
    }

    public void setMongoCollection(MongoCollection<T> mongoCollection) {
        this.mongoCollection = mongoCollection;
    }
}
