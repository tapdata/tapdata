package com.tapdata.tm.inspect.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.bean.Source;
import com.tapdata.tm.inspect.dto.InspectResultDto;
import com.tapdata.tm.inspect.entity.InspectResultEntity;
import com.tapdata.tm.inspect.param.SaveInspectResultParam;
import com.tapdata.tm.inspect.repository.InspectResultRepository;
import lombok.NonNull;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;

public abstract class InspectResultService extends BaseService<InspectResultDto, InspectResultEntity, ObjectId, InspectResultRepository> {
    public InspectResultService(@NonNull InspectResultRepository repository) {
        super(repository, InspectResultDto.class, InspectResultEntity.class);
    }
    public abstract Page<InspectResultDto> find(Filter filter, UserDetail userDetail, boolean inspectGroupByFirstCheckId);

    public abstract void joinResult(List<InspectResultDto> inspectResultDtos);

    public abstract void fillInspectInfo(List<InspectResultDto> inspectResultDtos);

    public abstract void setSourceConnectName(Source source, Map<String, DataSourceConnectionDto> connectionMap);

    public abstract InspectResultDto saveInspectResult(SaveInspectResultParam saveInspectResultParam, UserDetail userDetail);

    public abstract InspectResultDto getLatestInspectResult(ObjectId inspectId);

    public abstract InspectResultDto findById(Filter filter, UserDetail userDetail);

    public abstract InspectResultDto upsertInspectResultByWhere(Where where, SaveInspectResultParam saveInspectResultParam, UserDetail userDetail);

    public abstract void createAndPatch(InspectResultDto result);
}
