//package com.tapdata.tm.task.repository;
//
//import com.tapdata.tm.base.reporitory.BaseRepository;
//import com.tapdata.tm.task.entity.SubTaskEntity;
//import org.bson.types.ObjectId;
//import org.springframework.data.mongodb.core.MongoTemplate;
//import org.springframework.stereotype.Repository;
//
///**
// * @Author:
// * @Date: 2021/11/03
// * @Description:
// */
//@Repository
//public class SubTaskRepository extends BaseRepository<SubTaskEntity, ObjectId> {
//    public SubTaskRepository(MongoTemplate mongoOperations) {
//        super(SubTaskEntity.class, mongoOperations);
//    }
//
//    @Override
//    protected void init() {
//        super.init();
//    }
//}
