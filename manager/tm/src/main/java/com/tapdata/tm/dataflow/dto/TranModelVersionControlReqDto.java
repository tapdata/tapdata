package com.tapdata.tm.dataflow.dto;

import lombok.Data;

import java.util.List;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/10/30 下午5:32
 * @description
 */
@Data
public class TranModelVersionControlReqDto {

    private String stageId;
    private List<Stage> stages;

}
