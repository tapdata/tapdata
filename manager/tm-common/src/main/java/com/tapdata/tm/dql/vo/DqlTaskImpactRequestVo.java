package com.tapdata.tm.dql.vo;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class DqlTaskImpactRequestVo implements Serializable {
    private List<String> taskIds;
}
