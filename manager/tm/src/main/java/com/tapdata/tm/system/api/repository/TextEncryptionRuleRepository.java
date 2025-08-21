package com.tapdata.tm.system.api.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.system.api.entity.TextEncryptionRuleEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/8/11 14:40 Create
 * @description
 */
@Repository
public class TextEncryptionRuleRepository extends BaseRepository<TextEncryptionRuleEntity, ObjectId> {
    public TextEncryptionRuleRepository(MongoTemplate mongoOperations) {
        super(TextEncryptionRuleEntity.class, mongoOperations);
    }
}
