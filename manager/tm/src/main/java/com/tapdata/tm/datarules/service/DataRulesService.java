package com.tapdata.tm.datarules.service;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.bean.Rules;
import com.tapdata.tm.datarules.dto.DataRulesDto;
import com.tapdata.tm.datarules.entity.DataRulesEntity;
import com.tapdata.tm.datarules.repository.DataRulesRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.utils.MongoUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author:
 * @Date: 2021/10/19
 * @Description:
 */
@Service
@Slf4j
public class DataRulesService extends BaseService<DataRulesDto, DataRulesEntity, ObjectId, DataRulesRepository> {

    @Autowired
    private ModulesService modulesService;

    @Autowired
    private MetadataInstancesService metadataInstancesService;

    public DataRulesService(@NonNull DataRulesRepository repository) {
        super(repository, DataRulesDto.class, DataRulesEntity.class);
    }

    protected void beforeSave(DataRulesDto dataRules, UserDetail user) {
        Criteria criteria = Criteria.where("name").is(dataRules.getName());
        DataRulesDto dataRulesDto = findOne(new Query(criteria));
        if (dataRulesDto != null) {
            log.warn("name is exists. name = {}", dataRules.getName());
            throw new BizException("DataRules.RepeatName","name is exists.");
        }
    }


    public List<String> classifications(UserDetail user) {
        List<String> classification = repository.findDistinct(new Query(), "classification", user, String.class);
        return classification;
    }


    public List<Rules> getRules(String modelId) {
        ModulesDto modulesDto = modulesService.findById(MongoUtils.toObjectId(modelId));
        if (modulesDto == null) {
            log.warn("modules not found: model id = {}" + modelId);
            throw new BizException("DataRules.ModulesNotFound", "modules not found: " + modelId);
        }
        String tableName = modulesDto.getTableName();
        ObjectId connectionId =MongoUtils.toObjectId( modulesDto.getConnectionId());
        if (StringUtils.isNotBlank(tableName) && connectionId != null) {
            Criteria criteria = Criteria.where("source._id").is(connectionId.toHexString()).and("meta_type").is("collection")
                    .and("original_name").is(tableName).and("is_deleted").is(false);

            Query query = new Query(criteria);
            query.fields().include("data_rules");
            List<MetadataInstancesDto> metaDatas = metadataInstancesService.findAll(query);
            if (CollectionUtils.isNotEmpty(metaDatas)) {
                List<Rules> rules = metaDatas.get(0).getDataRules().getRules();
                return rules;
            }
        }
        return new ArrayList<>();
    }
}