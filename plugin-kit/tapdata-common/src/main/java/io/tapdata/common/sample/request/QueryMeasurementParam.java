package io.tapdata.common.sample.request;


import java.util.List;

public class QueryMeasurementParam {
   private List<QuerySampleParam> samples;
   private List<QueryStisticsParam> statistics;

   public List<QuerySampleParam> getSamples() {
      return samples;
   }

   public void setSamples(List<QuerySampleParam> samples) {
      this.samples = samples;
   }

   public List<QueryStisticsParam> getStatistics() {
      return statistics;
   }

   public void setStatistics(List<QueryStisticsParam> statistics) {
      this.statistics = statistics;
   }
}
