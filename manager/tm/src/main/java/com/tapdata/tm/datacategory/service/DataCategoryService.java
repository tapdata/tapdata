package com.tapdata.tm.datacategory.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.datacategory.dto.DataCategoryDto;
import com.tapdata.tm.datacategory.entity.DataCategoryEntity;
import com.tapdata.tm.datacategory.repository.DataCategoryRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.commons.util.MetaType;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2021/10/18
 * @Description:
 */
@Service
@Slf4j
public class DataCategoryService extends BaseService<DataCategoryDto, DataCategoryEntity, ObjectId, DataCategoryRepository> {

    @Autowired
    private MetadataInstancesService metadataInstancesService;
    @Autowired
    private DataSourceService dataSourceService;

    public DataCategoryService(@NonNull DataCategoryRepository repository) {
        super(repository, DataCategoryDto.class, DataCategoryEntity.class);
    }

    protected void beforeSave(DataCategoryDto dataCategory, UserDetail user) {
    }

    public void distinctTag() {

    }


//    public void handleTagId(Where where, Filter filter) {
//        List<Map> tags = null;
//        if (where != null) {
//            tags = (List<Map>) where.get("listtags");
//            filter = new Filter(where);
//        } else if (filter != null && filter.getWhere() != null) {
//            tags = (List<Map>)  filter.getWhere().get("listtags");
//        }
//
//        if (CollectionUtils.isNotEmpty(tags)) {
//            Set<String> ids = tags.stream().map(m -> (String) m.get("id")).collect(Collectors.toSet());
//            Criteria criteria = Criteria.where("meta_type").is(MetaType.collection.name()).and("is_deleted").is(false).and("classifications.id").in(ids);
//            List<MetadataInstancesDto> metas = metadataInstancesService.findAll(new Query(criteria));
//            if (CollectionUtils.isNotEmpty(metas)) {
//                List<Map<String, String>> connInfos = new ArrayList<>();
//                for (MetadataInstancesDto meta : metas) {
//                    Map<String, String> connInfo = new LinkedHashMap<>();
//                    connInfo.put("uri", meta.getSource().getDatabase_uri());
//                    connInfo.put("collection", meta.getOriginalName());
//                }
//                Map<String, Object> objectMap = new LinkedHashMap<>();
//                objectMap.put("$in", connInfos);
//
//                Where where1 = filter.getWhere();
//                if (where1 == null) {
//                    filter.setWhere(new Where());
//                }
//
//                filter.getWhere().put("conn_info", objectMap);
//                filter.getWhere().remove("listtags.id");
//
//            }
//
//        }
//
//    }
    public void handleTagId(Where where, Filter filter) {
        List<String> tags = null;
        if (where != null) {
            tags = (List<String>)  where.get("listtags");
            filter = new Filter(where);
            filter.setLimit(0);
            filter.setSkip(0);
        } else if (filter != null && filter.getWhere() != null) {
            tags = (List<String>)  filter.getWhere().get("listtags");
        }

        if (CollectionUtils.isNotEmpty(tags)) {
            Criteria criteria = Criteria.where("meta_type").is(MetaType.collection.name()).and("is_deleted").is(false).and("classifications.id").in(tags);
            List<MetadataInstancesDto> metas = metadataInstancesService.findAll(new Query(criteria));
            if (CollectionUtils.isNotEmpty(metas)) {
                List<Map<String, String>> connInfos = new ArrayList<>();
                for (MetadataInstancesDto meta : metas) {
                    Map<String, String> connInfo = new LinkedHashMap<>();
                    connInfo.put("uri", meta.getSource().getDatabase_uri());
                    connInfo.put("collection", meta.getOriginalName());
                }
                Map<String, Object> objectMap = new LinkedHashMap<>();
                objectMap.put("$in", connInfos);

                Where where1 = filter.getWhere();
                if (where1 == null) {
                    filter.setWhere(new Where());
                }

                filter.getWhere().put("conn_info", objectMap);
                filter.getWhere().remove("listtags.id");

            }

        }

    }


    // find if (ctx.method.name === 'find') {
    public void afterFind(List<DataCategoryDto> results) {
        Set<ObjectId> connectionIds = new HashSet<>();
        if (CollectionUtils.isNotEmpty(results)) {
            for (DataCategoryDto result : results) {
                connectionIds.add(result.getId());
            }

            Criteria criteria = Criteria.where("_id").in(connectionIds);
            Query query = new Query(criteria);
            query.fields().include("_id", "name", "status", "database_type");
            List<DataSourceConnectionDto> records = dataSourceService.findAll(query);
            Map<ObjectId, DataSourceConnectionDto> sourceMap = records.stream().collect(Collectors.toMap(DataSourceConnectionDto::getId, r -> r));
            for (DataCategoryDto result : results) {
                result.setSource(sourceMap.get(result.getConnectionId()));
            }
        }
    }


    public void getList(Filter filter) {
        handleTagId(null, filter);

        getData(filter);
    }

    private List<DataCategoryEntity> getData(Filter filter) {
        Where where = filter.getWhere();
        Criteria criteria = repository.whereToCriteria(where);
        AggregationOperation match = Aggregation.match(criteria);
        LookupOperation lookUp = LookupOperation.newLookup().
                from("Connections").
                localField("connection_id").
                foreignField("_id").
                as("source");

        ProjectionOperation project = Aggregation.project("id", "conn_info", "user_id", "database", "collection", "connection_id",
                "asset_desc", "total_docs", "violated_percentage", "create_time", "lastModified", "tags", "violated_docs",
                "source.id", "source.name", "source.status", "source.database_type");
        LimitOperation limitOperation = Aggregation.limit(filter.getLimit());
        SkipOperation skipOperation = Aggregation.skip((long)filter.getSkip());

        Sort sort = null;
        for (String field : filter.getSort()) {
            if (StringUtils.isNotBlank(field)){
                String[] str = field.split("\\s+");
                if(str.length == 2)
                    if (Sort.Direction.fromOptionalString(str[1]).isPresent()) {
                        Sort and = Sort.by(str[1], str[0]);
                        if (sort == null) {
                            sort = and;
                        } else {
                            sort.and(and);
                        }
                    }
            }
        }
        SortOperation sortOperation = null;
        if (sort != null) {
            sortOperation = Aggregation.sort(sort);
        }

        Aggregation aggregation = Aggregation.newAggregation(lookUp, match, project, sortOperation, skipOperation, limitOperation);
        List<DataCategoryEntity> dataCategorys = repository.getMongoOperations().aggregate(aggregation, "DataCategory", DataCategoryEntity.class).getMappedResults();

        return dataCategorys;

    }

    private long getCount(Filter filter) {
        handleTagId(null, filter);

        return getCountData(filter);
    }

    private long getCountData(Filter filter) {
        Where where = filter.getWhere();
        Criteria criteria = repository.whereToCriteria(where);
        AggregationOperation match = Aggregation.match(criteria);
        LookupOperation lookUp = LookupOperation.newLookup().
                from("Connections").
                localField("connection_id").
                foreignField("_id").
                as("source");
        CountOperation count = Aggregation.count().as("count");
        Aggregation aggregation = Aggregation.newAggregation(lookUp, match);
        List<Map> dataCategorys = repository.getMongoOperations().aggregate(aggregation, "DataCategory", Map.class).getMappedResults();
        Map map = dataCategorys.get(0);
        return (long) map.get("count");
    }

    //TODO 这里需要发送websocket. 暂时晚点实现
    public void analyzeByConnId() {

    }

}
