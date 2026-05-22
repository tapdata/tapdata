package com.tapdata.tm.lineage.analyzer.impl;

import com.tapdata.tm.lineage.analyzer.entity.LineageMetadataInstance;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.vo.SourceTypeEnum;
import com.tapdata.tm.modules.entity.ModulesEntity;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/20 16:58 Create
 * @description
 */
@Service("tableAnalyzerCustom")
public class TableAnalyzerCustom extends TableAnalyzerV1 {
    protected static final String[] METADATA_INCLUDE_FIELDS_CUSTOM  = new String[]{"_id", "sourceType","fields.tapType","fields.dataType","fields.fieldName","fields.originalFieldName","fields.primaryKey","fields.columnPosition","custom_properties","nodeId"};

    @Override
    protected String[] metadataIncludeFields() {
        return METADATA_INCLUDE_FIELDS_CUSTOM;
    }

    @Override
    protected LineageMetadataInstance getMetadata(String connectionId, String tableName) {
        Criteria baseCriteria = new Criteria("source._id").is(connectionId)
                .and("original_name").is(tableName);
        Criteria virtualCriteria = new Criteria("sourceType").is(SourceTypeEnum.VIRTUAL.name());
        Query query = Query.query(new Criteria().andOperator(baseCriteria, virtualCriteria));
        query.fields().include(metadataIncludeFields());
        LineageMetadataInstance lineageMetadataInstance = getMetadata(query);
        if (null == lineageMetadataInstance) {
            Criteria sourceCriteria = new Criteria("sourceType").is(SourceTypeEnum.SOURCE.name());
            query = Query.query(new Criteria().andOperator(baseCriteria, sourceCriteria));
            query.fields().include(metadataIncludeFields());
            lineageMetadataInstance = getMetadata(query);
        }
        return lineageMetadataInstance;
    }

    @Override
    protected LineageMetadataInstance getMetadata(Query query) {
        MetadataInstancesEntity metadataInstancesEntity = metadataInstancesRepository.findOne(query).orElse(null);
        if (null == metadataInstancesEntity || null == metadataInstancesEntity.getId()) {
            return null;
        }
        LineageMetadataInstance lineageMetadataInstance = new LineageMetadataInstance();
        lineageMetadataInstance.setNodeId(metadataInstancesEntity.getNodeId());
        lineageMetadataInstance.setFields(metadataInstancesEntity.getFields());
        lineageMetadataInstance.setCustomProperties(metadataInstancesEntity.getCustomProperties());
        lineageMetadataInstance.setId(metadataInstancesEntity.getId().toHexString());
        lineageMetadataInstance.setSourceType(metadataInstancesEntity.getSourceType());
        return lineageMetadataInstance;
    }

    @Override
    protected List<ModulesEntity> findModules(String connectionId, String table) {
        return new ArrayList<>();
    }
}
