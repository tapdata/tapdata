package io.tapdata.entity.event.control;

public class HeartbeatEvent extends ControlEvent {
    public static final int TYPE = 501;
    private Long referenceTime;

    public HeartbeatEvent() {
        super(TYPE);
    }

    public HeartbeatEvent referenceTime(Long referenceTime) {
        this.referenceTime = referenceTime;
        return this;
    }

    public Long getReferenceTime() {
        return referenceTime;
    }

    public void setReferenceTime(Long referenceTime) {
        this.referenceTime = referenceTime;
    }
    public HeartbeatEvent init() {
        time = System.currentTimeMillis();
        return this;
    }
}
