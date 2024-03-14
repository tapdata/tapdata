package com.tapdata.tm.ds.dto;

import lombok.Data;

import java.io.Serializable;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/9/8 下午5:14
 */
@Data
public class ConnectionStats implements Serializable {

    private long ready = 0L;
    private long testing = 0L;
    private long invalid = 0L;

    private long total = 0L;
}
