package com.tapdata.tm.base.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * 可调度的实体对象
 *
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/10/20 下午5:28
 * @description
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class SchedulableEntity extends BaseEntity {

    private String agentId; //调度到指定的实例上去
    private List<String> agentTags; // 标签

    private Integer scheduleTimes;  // 调度次数
    private Long scheduleTime;  // 上次调度时间

}
