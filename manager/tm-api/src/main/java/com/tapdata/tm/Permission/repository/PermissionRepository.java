package com.tapdata.tm.Permission.repository;

import lombok.Data;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/11 下午8:18
 * @description
 */
@Repository
@Data
public class PermissionRepository{
    protected final MongoTemplate mongoOperations;
    public PermissionRepository(MongoTemplate mongoOperations) {
        this.mongoOperations = mongoOperations;
    }

}
