package com.tapdata.tm.tcm.dto;

import lombok.Data;

import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/11/22 下午7:48
 */
@Data
public class PaidPlanRes {

    private Map<String, Integer> limit;
}
