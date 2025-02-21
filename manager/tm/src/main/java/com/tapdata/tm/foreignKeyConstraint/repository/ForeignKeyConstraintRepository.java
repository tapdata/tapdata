package com.tapdata.tm.foreignKeyConstraint.repository;


import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.foreignKeyConstraint.entity.ForeignKeyConstraintEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ForeignKeyConstraintRepository extends BaseRepository<ForeignKeyConstraintEntity, ObjectId> {
    public ForeignKeyConstraintRepository(MongoTemplate mongoOperations) {
        super(ForeignKeyConstraintEntity.class, mongoOperations);
    }
}
