package com.tapdata.tm.dql.vo;

import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.dto.DqlRecoveryBatchDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class DqlEventDetailVo extends DqlEventDto {
    private DqlRecoveryBatchDto currentBatch;
}
