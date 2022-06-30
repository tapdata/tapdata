package com.tapdata.tm.ds.repository;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.ds.entity.DataSourceDefinitionEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import static com.tapdata.tm.utils.MongoUtils.*;

/**
 * @Author: Zed
 * @Date: 2021/8/24
 * @Description:
 */
@Repository
public class DataSourceDefinitionRepository extends BaseRepository<DataSourceDefinitionEntity, ObjectId> {
    public DataSourceDefinitionRepository(MongoTemplate mongoOperations) {
        super(DataSourceDefinitionEntity.class, mongoOperations);
    }


    public Criteria filterBuildCriteria(Filter filter) {
        return buildCriteria(filter.getWhere(), entityInformation);
    }

    public Query filterToSimpleQuery(Filter filter) {
        if (filter == null)
            filter = new Filter();

        final Query query = new Query().cursorBatchSize(cursorBatchSize);

        if (filter.getLimit() > 0)
            query.limit(filter.getLimit());
        if (filter.getSkip() > 0)
            query.skip(filter.getSkip());

        applyField(query, filter.getFields());
        applySort(query, filter.getSort());
        return query;
    }
}
