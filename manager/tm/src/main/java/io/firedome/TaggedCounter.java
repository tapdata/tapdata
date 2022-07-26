package io.firedome;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.HashMap;
import java.util.Map;

public class TaggedCounter {
    private String name;
    private String tagName;
    private MeterRegistry registry;
    private Map<String, Counter> counters = new HashMap<>();

    public TaggedCounter(String name, String tagName, MeterRegistry registry) {
        this.name = name;
        this.tagName = tagName;
        this.registry = registry;
    }

    public void increment(String tagValue){
        Counter counter = counters.get(tagValue);
        if(counter == null) {
            counter = Counter.builder(name).tags(tagName, tagValue).register(registry);
            counters.put(tagValue, counter);
        }
        counter.increment();
    }
}
