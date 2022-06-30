package com.tapdata.tm.inspect.dto;

import lombok.Data;

import java.util.List;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/24 上午11:06
 * @description
 */
@Data
public class InspectDataSource {

    private String connectionId; //"": "",  // 连接ID
    private String table;        //"": "IV_JW_MATRL",  // 不能带 owner，默认使用connection的owner
    private String sortColumn;   //": "INVNT_ID",  // 必填，提示使用索引字段
    private String direction;    //": "ASC",  // 固定值 ASC，后端强制ASC
    private List<String> columns; //[  // 必须带有排序字段，前端可以做限制，后端要自动适配
                                        //"INVNT_ID", "...", // 需要校验比对的列名称，必填，顺序与目标字段一致
                                    //],
    private Integer limit;// : null, // 取多少行数据，默认全部
    private Integer skip; //": null,  // 跳过多少行数据
    private String where; //": "",   // sql 查询条件，直接拼接到sql中, MongoDB where 查询条件 json string
        // 默认为 null，用户可以选择使用自定义 sql，但必须包含排序字段
        // "sql": null, // 与上面几个配置互斥 暂时不支持自定义

    private List<Field> fields;

    @Data
    public static class Field {
        private String field_name;
        private int primary_key_position;
        private String id;
    }
}
