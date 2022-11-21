package com.tapdata.tm.license.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;


/**
 * License
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("License")
public class LicenseEntity extends BaseEntity {

    private String hostname;

    private String license;

    private String sid;

    private Long expires_on;

    private Date expirationDate;


}