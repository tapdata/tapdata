//package com.tapdata.tm.monitor.entity;
//
//import com.tapdata.tm.monitor.dto.Sample;
//import io.tapdata.common.utils.NumberUtils;
//import lombok.Data;
//import org.springframework.data.mongodb.core.mapping.Document;
//import org.springframework.data.mongodb.core.mapping.Field;
//
//import java.util.Date;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
////@Document("TMMeasurement")
//@Data
//@Document("TMMeasurement")
//public class TMMeasurementEntity {
//    private Date date;
//    private Map<String, String> tags;
//    private Date first;
//    private Date last;
//
//    @Field("ss")
//    private List<Sample> samples;
//
//    public Map<String, Number> averageValues() {
//        Map<String, Number> finalKeyValueMap = new HashMap<>();
//        Map<String, Number> keyValueMap = new HashMap<>();
//        Map<String, Integer> keyCounterMap = new HashMap<>();
//        if(samples != null) {
//            for(Sample sample : samples) {
//                Map<String, Number> map = sample.getValues();
//                for(Map.Entry<String, Number> entry : map.entrySet()) {
//                    Number number = keyValueMap.get(entry.getKey());
//                    if(number == null) {
//                        number = entry.getValue();
//                    } else {
//                        number = NumberUtils.addNumbers(number, entry.getValue());
//                    }
//                    keyValueMap.put(entry.getKey(), number);
//
//                    Integer counter = keyCounterMap.get(entry.getKey());
//                    if(counter != null) {
//                        counter++;
//                    } else {
//                        counter = 1;
//                    }
//                    keyCounterMap.put(entry.getKey(), counter);
//                }
//            }
//            if(!keyValueMap.isEmpty()) {
//                for(Map.Entry<String, Number> entry : keyValueMap.entrySet()) {
//                    Integer counter = keyCounterMap.get(entry.getKey());
//                    if(counter != null && counter > 0) {
//                        finalKeyValueMap.put(entry.getKey(), NumberUtils.divideNumbers(entry.getValue(), counter));
//                    }
//                }
//            }
//        }
//        return finalKeyValueMap;
//    }
//
//}
