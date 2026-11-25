package com.tapdata.tm.commons.throttle;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Per-phase throttling configuration (applies independently to input and output within the phase).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ThrottlePhaseLimit implements Serializable {
    private static final long serialVersionUID = 1L;

    private boolean enabled;
    private Long bytesPerSec;
    private Long eventsPerSec;
}
