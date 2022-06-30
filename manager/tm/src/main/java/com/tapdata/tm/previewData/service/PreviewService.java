package com.tapdata.tm.previewData.service;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.previewData.param.PreviewParam;
import com.tapdata.tm.utils.MongoUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@Slf4j
public class PreviewService {

    @Autowired
    MetadataInstancesService metadataInstancesService;

    @Autowired
    ModulesService modulesService;

    @Autowired
    DataSourceService dataSourceService;

    public Map preview(PreviewParam previewParam) {
        Map result = new HashMap();
        String id = previewParam.getId();
        MetadataInstancesDto metadataInstancesDto = metadataInstancesService.findById(MongoUtils.toObjectId(id));
        if (!"collection".equals(metadataInstancesDto.getMetaType()) && !"api".equals(metadataInstancesDto.getMetaType())) {
            log.info("只支持预览  api 和collection类型");
            return result;
        }
        SourceDto sourceDto = metadataInstancesDto.getSource();
        if (!"mongodb".equals(sourceDto.getDatabase_type())) {
            log.info("只支持预览 mongodb");
            return result;
        }

        String url = "";
        String col = "";
        String databaseName = "";
        if ("collection".equals(metadataInstancesDto.getMetaType())) {
            url = sourceDto.getDatabase_uri();
            col = metadataInstancesDto.getOriginalName();
            databaseName = sourceDto.getDatabase_name();
        } else if ("api".equals(metadataInstancesDto.getMetaType())) {
            String sourceId = sourceDto.get_id();
            if (StringUtils.isEmpty(sourceId)) {
                sourceId = sourceDto.getId().toString();
            }
            ModulesDto modulesDto = modulesService.findById(MongoUtils.toObjectId(sourceId));
            DataSourceConnectionDto dataSourceConnectionDto = dataSourceService.findById(MongoUtils.toObjectId(modulesDto.getDataSource()));
            url = dataSourceConnectionDto.getDatabase_uri();
            col = modulesDto.getTableName();
            databaseName = dataSourceConnectionDto.getDatabase_name();
        }

        MongoTemplate mongoTemplate = getMongotemplate(url);

        Long limit = previewParam.getLimit();
        Long skip = previewParam.getSkip();
        Query query = new Query().skip((skip / limit) + 1).limit(limit.intValue()).with(Sort.by("_id").descending());
        List<Document> documentList = mongoTemplate.find(query, Document.class, col);

        Set<String> headSet = new HashSet();
        List<Map<String,String>> headList=new ArrayList<>();
        if (CollectionUtils.isNotEmpty(documentList)) {
            documentList.forEach(document -> {
                document.put("_id",document.remove("_id").toString());
                headSet.addAll(document.keySet());
            });
            for(String text : headSet){
                Map<String,String> headMap=new HashMap<>();
                headMap.put("text",text );
                headMap.put("value",text);
                headList.add(headMap);
            }
        }
        /**
         * spring-data-mongodb-2.20以上 MongoTemplate.count()  计数不对   只能这样处理
         */
        BasicQuery basicQuery = new BasicQuery(query.getQueryObject().toJson());
        Long total = mongoTemplate.count(basicQuery, col);
        Map data = new HashMap();

        data.put("total", total);
        data.put("head", headList);
        data.put("items", documentList);
        result.put("data", data);
        result.put("msg", "ok");
        result.put("code", "ok");
        result.put("ts",new Date().getTime());
        return result;

    }


    private MongoTemplate getMongotemplate(String url) {
        MongoTemplate mongoTemplate = null;
        if (mongoTemplate == null) {
            mongoTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(url));
        }
        return mongoTemplate;
    }
  /*  private MongoTemplate getMongotemplate(String url,String databaseName) {
        MongoClient mongoClient = new MongoClient(url);
            mongoTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(mongoClient,databaseName));
        return mongoTemplate;
    }*/
}
