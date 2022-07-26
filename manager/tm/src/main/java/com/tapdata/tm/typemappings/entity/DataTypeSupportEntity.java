package com.tapdata.tm.typemappings.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/11 下午2:05
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Document("dataTypeSupport")
@CompoundIndex(def = "{'sourceDbType': 1, 'targetDbType', 1, }")
public class DataTypeSupportEntity extends BaseEntity {

    private String sourceDbType;
    private List<String> targetDbType;
    private String operator;
    private Object expression;
    private boolean support;
}
