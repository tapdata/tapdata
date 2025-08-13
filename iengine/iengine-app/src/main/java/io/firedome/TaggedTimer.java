package io.firedome;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class TaggedTimer {
    private String name;
    private String tagName;
    private MeterRegistry registry;
    private Map<String, Timer> timers = new HashMap<>();

    public TaggedTimer(String name, String tagName, MeterRegistry registry) {
        this.name = name;
        this.tagName = tagName;
        this.registry = registry;
    }

    public Timer getTimer(String tagValue){
        Timer timer = timers.get(tagValue);
        if(timer == null) {
            timer = Timer.builder(name).tags(tagName, tagValue)
                    .serviceLevelObjectives(
                            Duration.ofMillis(100),
                            Duration.ofMillis(300),
                            Duration.ofMillis(500),
                            Duration.ofMillis(800),
                            Duration.ofSeconds(1),
                            Duration.ofSeconds(5)
                    ).register(registry);
            timers.put(tagValue, timer);
        }
        return timer;
    }

}
