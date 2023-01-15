package com.tapdata.tm.behavior.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/6/21 下午4:18
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class BehaviorDto extends BaseDto {

    private String dataFlowId;
    private String agentId;
    private String externalUserId;

    private String code;// 行为代码

    private String period; // 时段，精确到小时

    private Map<String, Object> attrs;

    private long lastUpdateTime = 0;

}
