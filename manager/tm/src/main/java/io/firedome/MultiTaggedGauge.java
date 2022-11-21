package io.firedome;

import io.micrometer.core.instrument.*;

import java.util.*;

public class MultiTaggedGauge {

    private String name;
    private String[] tagNames;
    private MeterRegistry registry;
    private Map<String, DoubleWrapper> gaugeValues = new HashMap<>();

    public MultiTaggedGauge(String name, MeterRegistry registry, String ... tags) {
        this.name = name;
        this.tagNames = tags;
        this.registry = registry;
    }

    public void set(double value, String ... tagValues){
        String valuesString = Arrays.toString(tagValues);
        if(tagValues.length != tagNames.length) {
            throw new IllegalArgumentException("Gauge tags mismatch! Expected args are "+Arrays.toString(tagNames)+", provided tags are "+valuesString);
        }

        DoubleWrapper number = gaugeValues.get(valuesString);
        if(number == null) {
            List<Tag> tags = new ArrayList<>(tagNames.length);
            for(int i = 0; i<tagNames.length; i++) {
                tags.add(new ImmutableTag(tagNames[i], tagValues[i]));
            }
            DoubleWrapper valueHolder = new DoubleWrapper(value);
            Gauge.builder(name, valueHolder, DoubleWrapper::getValue).tags(tags).register(registry);
            gaugeValues.put(valuesString, valueHolder);
        } else {
            number.setValue(value);
        }
    }
}
