package com.tapdata.tm.worker.dto;

import com.tapdata.tm.cluster.dto.SystemInfo;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/13 下午9:26
 * @description
 */
@Data
public class WorkerProcessInfoDto {

    private int runningNum;
    private Map<String, Long> runningTaskNum;
    private List<DataFlowDto> dataFlows;
    private SystemInfo systemInfo;

}
