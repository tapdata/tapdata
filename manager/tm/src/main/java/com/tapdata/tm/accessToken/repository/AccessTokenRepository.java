package com.tapdata.tm.accessToken.repository;

import lombok.Data;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author: Zed
 * @Date: 2021/8/24
 * @Description:
 */
@Repository
@Data
public class AccessTokenRepository {

    protected final MongoTemplate mongoOperations;
}
