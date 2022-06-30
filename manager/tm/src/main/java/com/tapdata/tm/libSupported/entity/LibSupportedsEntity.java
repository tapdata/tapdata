package com.tapdata.tm.libSupported.entity;

import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * LibSupporteds
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("LibSupporteds")
public class LibSupportedsEntity extends BaseEntity {

    private String databaseType;

    private Object supportedList;

}