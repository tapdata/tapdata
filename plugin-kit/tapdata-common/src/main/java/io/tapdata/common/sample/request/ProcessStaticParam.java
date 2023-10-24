package io.tapdata.common.sample.request;

import java.util.Map;

public class ProcessStaticParam {
   private Map<String,String> tags;
   private Map values;

   public Map<String, String> getTags() {
      return tags;
   }

   public void setTags(Map<String, String> tags) {
      this.tags = tags;
   }

   public Map getValues() {
      return values;
   }

   public void setValues(Map values) {
      this.values = values;
   }
}
