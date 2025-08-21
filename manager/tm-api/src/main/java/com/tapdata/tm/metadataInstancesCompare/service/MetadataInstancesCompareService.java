package com.tapdata.tm.metadataInstancesCompare.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.MetadataInstancesCompareDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadataInstancesCompare.entity.MetadataInstancesCompareEntity;
import com.tapdata.tm.metadataInstancesCompare.param.MetadataInstancesApplyParam;
import com.tapdata.tm.metadataInstancesCompare.repository.MetadataInstancesCompareRepository;
import com.tapdata.tm.metadatainstance.vo.MetadataInstancesCompareResult;
import lombok.NonNull;
import org.bson.types.ObjectId;

import java.util.List;

public abstract class MetadataInstancesCompareService extends BaseService<MetadataInstancesCompareDto, MetadataInstancesCompareEntity, ObjectId, MetadataInstancesCompareRepository> {
    public MetadataInstancesCompareService(@NonNull MetadataInstancesCompareRepository repository) {
        super(repository, MetadataInstancesCompareDto.class, MetadataInstancesCompareEntity.class);
    }

    @Override
    protected void beforeSave(MetadataInstancesCompareDto dto, UserDetail userDetail) {

    }

    public abstract void saveMetadataInstancesCompareApply(List<MetadataInstancesApplyParam> metadataInstancesCompareDtos, UserDetail userDetail, Boolean all, String nodeId);
    public abstract void deleteMetadataInstancesCompareApply(List<MetadataInstancesApplyParam> metadataInstancesCompareDtos, UserDetail userDetail, Boolean all, String nodeId);
    public abstract MetadataInstancesCompareResult getMetadataInstancesCompareResult(String nodeId,String taskId,String tableFilter,int page, int pageSize);
    public abstract List<String> getApplyRules(String nodeId,String taskId);
}
