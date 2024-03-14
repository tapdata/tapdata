package com.tapdata.tm.inspect.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.service.IBaseService;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.bean.Source;
import com.tapdata.tm.inspect.dto.InspectResultDto;
import com.tapdata.tm.inspect.entity.InspectResultEntity;
import com.tapdata.tm.inspect.param.SaveInspectResultParam;
import com.tapdata.tm.inspect.repository.InspectResultRepository;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;

public interface InspectResultService extends IBaseService<InspectResultDto, InspectResultEntity, ObjectId, InspectResultRepository> {
    Page<InspectResultDto> find(Filter filter, UserDetail userDetail, boolean inspectGroupByFirstCheckId);

    void joinResult(List<InspectResultDto> inspectResultDtos);

    void fillInspectInfo(List<InspectResultDto> inspectResultDtos);

    void setSourceConnectName(Source source, Map<String, DataSourceConnectionDto> connectionMap);

    InspectResultDto saveInspectResult(SaveInspectResultParam saveInspectResultParam, UserDetail userDetail);

    InspectResultDto getLatestInspectResult(ObjectId inspectId);

    InspectResultDto findById(Filter filter, UserDetail userDetail);

    InspectResultDto upsertInspectResultByWhere(Where where, SaveInspectResultParam saveInspectResultParam, UserDetail userDetail);

    void createAndPatch(InspectResultDto result);
}
