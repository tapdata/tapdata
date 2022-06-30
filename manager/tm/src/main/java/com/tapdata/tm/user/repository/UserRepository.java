package com.tapdata.tm.user.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.user.entity.User;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/11 下午8:18
 * @description
 */
@Repository
public class UserRepository extends BaseRepository<User, ObjectId> {
    public UserRepository(MongoTemplate mongoOperations) {
        super(User.class, mongoOperations);
    }


}
