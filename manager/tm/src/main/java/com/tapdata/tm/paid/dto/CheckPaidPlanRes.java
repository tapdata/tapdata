package com.tapdata.tm.paid.dto;

import com.tapdata.tm.commons.task.dto.Message;
import lombok.Data;

import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/11/22 下午8:48
 */
@Data
public class CheckPaidPlanRes {

    private boolean isValid;
    private Map<String, String> messages;

    private int limit;
    private long current;
}
