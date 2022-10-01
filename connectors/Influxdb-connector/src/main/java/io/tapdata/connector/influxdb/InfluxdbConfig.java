package io.tapdata.connector.influxdb;

import io.tapdata.common.CommonDbConfig;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Getter
@NoArgsConstructor
public class InfluxdbConfig extends CommonDbConfig implements Serializable {
    private static final long serialVersionUID = -2L;
    
    /** InfluxDB批量刷写Line数据的数量**/
    private Integer maxinumLinesPerRequest = 10;
    
    /**Influxdb定义序列化器 **/
    private String deserializer;
    
    /** Influxdb保留策略**/
    private String retension;
    
    /** 批量刷写元素的容量**/
    private String ingestQueueCapacity;
    
    /** 是否开启批量刷鞋 **/
    private int batchActions;
    
    /** 刷写的时间间隔 **/
    private int flushDuration;
    
    /** 刷写时间间隔的单位 **/
    private TimeUnit flushDurationTimeUnit;
    
    private Boolean enableGzip;
    
    /** 是否创建数据库**/
    private boolean createDatabase;
    
    public InfluxdbConfig load(Map<String, Object> map) {
        return (InfluxdbConfig) super.load(map);
    }
    
    public boolean isEnableGzip(){
        return enableGzip;
    }
    
    
}
