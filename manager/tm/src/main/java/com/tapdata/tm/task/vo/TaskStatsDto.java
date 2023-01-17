package com.tapdata.tm.task.vo;

import lombok.Data;

import java.util.Map;

/**
 *
 * Task stat data transport object
 *
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/9/2 上午10:41
 */
@Data
public class TaskStatsDto {

    private Map<String, Long> taskTypeStats;

}
