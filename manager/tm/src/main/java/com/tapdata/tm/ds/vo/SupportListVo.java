package com.tapdata.tm.ds.vo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

/**
 * [
 * {
 * name: 'mysql',
 * supportList: [
 * {
 * version: '8.0',
 * isSupportValification: true,
 * canBeTarget: true,
 * canBeSource: true
 * <p>
 * },
 * {
 * version: '9.0'
 * }
 * ]
 * }
 * ]
 */
@AllArgsConstructor
@Getter
@Setter
public class SupportListVo {
    private String name;

    private List supportList;

    public  SupportListVo(List<Map<String,Object>> supportList){
        this.supportList=supportList;
    }

}
