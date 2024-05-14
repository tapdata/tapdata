package com.tapdata.tm.inspect.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.bean.Source;
import com.tapdata.tm.inspect.dto.InspectResultDto;
import com.tapdata.tm.inspect.param.SaveInspectResultParam;
import com.tapdata.tm.inspect.repository.InspectResultRepository;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @Author:
 * @Date: 2021/09/14
 * @Description:
 */
@Service
@Slf4j
public class InspectResultServiceImpl extends InspectResultService{
    public InspectResultServiceImpl(@NonNull InspectResultRepository repository) {
        super(repository);
    }

    @Override
    protected void beforeSave(InspectResultDto dto, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Page<InspectResultDto> find(Filter filter, UserDetail userDetail, boolean inspectGroupByFirstCheckId) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void joinResult(List<InspectResultDto> inspectResultDtos) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void fillInspectInfo(List<InspectResultDto> inspectResultDtos) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void setSourceConnectName(Source source, Map<String, DataSourceConnectionDto> connectionMap) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public InspectResultDto saveInspectResult(SaveInspectResultParam saveInspectResultParam, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public InspectResultDto getLatestInspectResult(ObjectId inspectId) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public InspectResultDto findById(Filter filter, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public InspectResultDto upsertInspectResultByWhere(Where where, SaveInspectResultParam saveInspectResultParam, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void createAndPatch(InspectResultDto result) {
        throw new BizException("TapOssNonSupportFunctionException");
    }
}
