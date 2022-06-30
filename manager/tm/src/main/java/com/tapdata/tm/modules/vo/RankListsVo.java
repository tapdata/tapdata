package com.tapdata.tm.modules.vo;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class RankListsVo {
    //失败率排行
    List<Map> items;
    Long total;
    Number totalPage;
}




