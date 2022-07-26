package io.firedome;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.HashMap;
import java.util.Map;

public class TaggedGauge {
    private String name;
    private String tagName;
    private MeterRegistry registry;
    private Map<String, DoubleWrapper> gaugeValues = new HashMap<>();

    public TaggedGauge(String name, String tagName, MeterRegistry registry) {
        this.name = name;
        this.tagName = tagName;
        this.registry = registry;
    }

    public void set(String tagValue, double value){
        DoubleWrapper number = gaugeValues.get(tagValue);
        if(number == null) {
            DoubleWrapper valueHolder = new DoubleWrapper(value);
            Gauge.builder(name, valueHolder, DoubleWrapper::getValue).tags(tagName, tagValue).register(registry);
            gaugeValues.put(tagValue, valueHolder);

        } else {
            number.setValue(value);
        }
    }
}
