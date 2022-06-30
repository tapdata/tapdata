package com.tapdata.tm.typemappings.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;


/**
 * TypeMappings
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("TypeMappings")
public class TypeMappingsEntity extends BaseEntity implements Serializable {

   private String databaseType;

   private String version;

   private String dbType;

   private Long minPrecision;

   private Long maxPrecision;

   private Long minScale;

   private Long maxScale;

   private String tapType;

   private boolean dbTypeDefault;
   private boolean tapTypeDefault;

   private Boolean fixed;
   private String direction;
   private String getter;
   private String minValue;
   private String maxValue;
   private int code;

}
