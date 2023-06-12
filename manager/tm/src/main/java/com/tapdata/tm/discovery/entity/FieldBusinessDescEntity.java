package com.tapdata.tm.discovery.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;
import java.util.Map;


@EqualsAndHashCode(callSuper = true)
@Data
@Document("FieldBusinessDesc")
public class FieldBusinessDescEntity extends BaseEntity {
    private String metadataInstanceId;

    Map<String,String> fieldBusinessDesc;

}
