package io.tapdata.common.sample.request;


import java.util.Date;
import java.util.Map;

public class GetMeasurementParam {

   public Map<String, String> getTags() {
      return tags;
   }

   public void setTags(Map<String, String> tags) {
      this.tags = tags;
   }

   public Date getDate() {
      return date;
   }

   public void setDate(Date date) {
      this.date = date;
   }

   public String getGranularity() {
      return granularity;
   }

   public void setGranularity(String granularity) {
      this.granularity = granularity;
   }

   public String getMeasurement() {
      return measurement;
   }

   public void setMeasurement(String measurement) {
      this.measurement = measurement;
   }

   private Map<String,String> tags;
   private Date date;
   private String granularity;
   private String measurement;
}
