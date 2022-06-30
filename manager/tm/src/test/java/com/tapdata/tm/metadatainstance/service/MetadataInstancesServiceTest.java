package com.tapdata.tm.metadatainstance.service;

import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.util.MetaType;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.metadatainstance.vo.TableSupportInspectVo;
import com.tapdata.tm.utils.MongoUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.Map;

class MetadataInstancesServiceTest extends BaseJunit {

    @Autowired
    MetadataInstancesService metadataInstancesService;

    @Autowired
    MetadataInstancesRepository metadataInstancesRepository;

    @Test
    void afterPropertiesSet() {
    }

    @Test
    void beforeSave() {
    }

    @Test
    void findAll() {
        String filterJson = "{\"where\":{\"meta_type\":{\"inq\":[\"table\",\"collection\"]},\"source._id\":{\"inq\":[\"61b1abb3b91267566bca4ad9\",\"6177d3b6f2cae963d28bd6a6\"]}},\"fields\":{\"id\":true,\"name\":true,\"original_name\":true,\"source\":true,\"source.id\":true,\"source.name\":true,\"fields\":true,\"fields.id\":true,\"fields.field_name\":true,\"fields.primary_key_position\":true,\"databaseId\":true,\"meta_type\":true}}";
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        printResult(metadataInstancesService.find(filter));

    }

    @Test
    void jobStats() {
    }

    @Test
    void schema() {
    }

    @Test
    void lineage() {
    }

    @Test
    void afterFindOne() {
    }

    @Test
    void afterFindAll() {
    }

    @Test
    void afterFind() {
    }

    @Test
    void testAfterFind() {
    }

    @Test
    void classifications() {
    }

    @Test
    void beforeUpdateById() {
    }

    @Test
    void afterUpdateById() {
    }

    @Test
    void compareHistory() {
    }

    @Test
    void tableConnection() {
    }

    @Test
    void loadSchema() {
    }

    @Test
    void findMetadata() {
        Long start = System.currentTimeMillis();
        List<MetadataInstancesDto> metadataInstancesTable = metadataInstancesService.findAll(Query.query(Criteria.where("source._id").is("62394713decec63c31d0c97a")
                .and("is_deleted").ne(true)
                .orOperator(Criteria.where("meta_type").is(MetaType.table.toString()), Criteria.where("meta_type").is(MetaType.collection.toString()))));
        System.out.println("总记录数： " + metadataInstancesTable.size());

        Long end = System.currentTimeMillis();
        printResult("总时间： " + String.valueOf(end - start));
    }

    @Test
    public void tableSupportInspect() {
        TableSupportInspectVo list = metadataInstancesService.tableSupportInspect("622c575b2565ef5315f4b214", "pg_cdc2");
        printResult(list);
    }


    @Test
    public void IdSort() {
        Query query = Query.query(Criteria.where("original_name").regex("pk").and("is_deleted").ne(true).and("_id").gte(MongoUtils.toObjectId("627b3d48410b7e962483216f")));
        query.with(Sort.by("_id").descending());
        query.limit(16);
        List<MetadataInstancesDto> metadatas = metadataInstancesService.findAllDto(query, getUser("62172cfc49b865ee5379d3ed"));
        metadatas.forEach(metadataInstancesDto -> {
            System.out.println(metadataInstancesDto.getId().toString());
        });
    }

    @Test
    public void search() {
        List<Map<String, Object>> metadataInstancesDtoList=   metadataInstancesService.search("table", "pk3", "624faa14e216e6f75ae7dfe5", 3, getUser("62172cfc49b865ee5379d3ed"));
        for (Map<String, Object> metadataInstancesDto : metadataInstancesDtoList) {
            System.out.println(metadataInstancesDto.get("id"));
        }
    }

}