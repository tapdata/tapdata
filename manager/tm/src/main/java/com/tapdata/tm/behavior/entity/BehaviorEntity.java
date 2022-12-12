package com.tapdata.tm.behavior.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/6/21 下午2:46
 */
@Document("Behavior")
@Setter
@Getter
@CompoundIndex(def = "{'dataFlowId': 1, 'code': 1, 'period': 1}")
@CompoundIndex(def = "{'taskId': 1, 'code': 1, 'period': 1}")
public class BehaviorEntity extends BaseEntity {

    @Indexed
    private String dataFlowId;
    @Indexed
    private String taskId;
    private String agentId;
    private String externalUserId;

    @Indexed
    private String code;// 行为代码

    @Indexed
    private String period; // 时段，精确到小时

    private Map<String, Object> attrs;

    private long lastUpdateTime = 0;

    private long counter;

}
