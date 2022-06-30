package com.tapdata.tm.customNode.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Map;


/**
 * Logs
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("CustomNodeTemps")
public class CustomNodeEntity extends BaseEntity {
  private String name;
  private String desc;
  private String icon;
  private Map<String,Object> formSchema;
  private String template;

}
