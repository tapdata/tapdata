package com.tapdata.tm.monitor.param;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/22 18:59 Create
 */
@Data
public class TablesParam implements Serializable {
    private List<String> tables;

}
