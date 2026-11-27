package com.tapdata.tm.verison.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;


/**
 * Version of the tapdata
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("Version")
public class VersionEntity extends BaseEntity {
    private String type;
    private String version;
    private List<String> executedPatches;

}