package io.firedome;

import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
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
            timer = Timer.builder(name).tags(tags)
//                    .publishPercentileHistogram()
                    .serviceLevelObjectives(
                            Duration.ofMillis(100),
                            Duration.ofMillis(300),
                            Duration.ofMillis(500),
                            Duration.ofMillis(800),
                            Duration.ofSeconds(1),
                            Duration.ofSeconds(5)
                    )
                    .register(registry);
            timers.put(valuesString, timer);
        }
        return timer;
    }

}
