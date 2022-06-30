
package com.tapdata.tm.commons.task.dto;


import lombok.Data;

import java.io.Serializable;

@Data
public class JoinSetting implements Serializable {

    private String cacheKey;
    private String sourceKey;
}
