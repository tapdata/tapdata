package com.tapdata.tm.cluster.dto;

import lombok.*;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/13 下午9:59
 * @description
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class Component {
    private String status;
    private String processID;
}
