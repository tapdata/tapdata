package com.tapdata.tm.modules.param;

import lombok.Data;

@Data
public class RankListsParam {
    private String order ;  //asc 升序  desc 降序
    private Integer page;
    private String type;
}
