package com.tapdata.tm.commons.schema.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * @Author: Zed
 * @Date: 2021/9/11
 * @Description:
 */
@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
public class Schema {
    private List<Table> tables;
}
