package com.tapdata.tm.observability.service;

import com.tapdata.tm.observability.dto.ServiceMethodDto;
import com.tapdata.tm.observability.vo.ParallelInfoVO;

import java.util.List;

public interface ObservabilityService {
    List<ParallelInfoVO<?>> getList(List<ServiceMethodDto<?>> serviceMethodDto);
}
