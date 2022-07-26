package io.firedome;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.Timer;

import java.util.*;


public class MultiTaggedTimer {
    private String name;
    private String[] tagNames;
    private MeterRegistry registry;
    private Map<String, Timer> timers = new HashMap<>();

    public MultiTaggedTimer(String name, MeterRegistry registry, String ... tags) {
        this.name = name;
        this.tagNames = tags;
        this.registry = registry;
    }

    public Timer getTimer(String ... tagValues){
        String valuesString = Arrays.toString(tagValues);
        if(tagValues.length != tagNames.length) {
            throw new IllegalArgumentException("Timer tags mismatch! Expected args are "+Arrays.toString(tagNames)+", provided tags are "+valuesString);
        }
        Timer timer = timers.get(valuesString);
        if(timer == null) {
            List<Tag> tags = new ArrayList<>(tagNames.length);
            for(int i = 0; i<tagNames.length; i++) {
                tags.add(new ImmutableTag(tagNames[i], tagValues[i]));
            }
            timer = Timer.builder(name).tags(tags).register(registry);
            timers.put(valuesString, timer);
        }
        return timer;
    }

}
