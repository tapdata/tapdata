package io.tapdata.dql.recovery;

import com.tapdata.entity.TapdataCountDownLatchEvent;
import com.tapdata.entity.TapdataDqlRecoveryEvent;
import com.tapdata.entity.TapdataEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlSourceReadGateTest {
    @Test
    void pausesNormalSourceReadsAndAllowsOnlyRecoveryTraffic() throws Exception {
        DqlSourceReadGate gate = new DqlSourceReadGate();
        TapdataEvent normalEvent = new TapdataEvent();

        gate.open();
        assertTrue(gate.allow(normalEvent));
        gate.release(normalEvent);

        gate.beginPausing();
        assertFalse(gate.allow(normalEvent));
        assertTrue(gate.awaitDrained(100, java.util.concurrent.TimeUnit.MILLISECONDS));

        gate.enterRecoveryOnly();
        assertTrue(gate.allow(TapdataDqlRecoveryEvent.createBegin("batch-1")));
        assertTrue(gate.allow(TapdataCountDownLatchEvent.create(1)));
        assertFalse(gate.allow(normalEvent));

        gate.beginResuming();
        assertFalse(gate.allow(normalEvent));
        assertFalse(gate.allow(TapdataDqlRecoveryEvent.createEnd("batch-1")));

        gate.close();
        assertTrue(gate.allow(normalEvent));
    }

    @Test
    void waitsForAcceptedNormalEnqueueToFinishBeforeRecoveryOnly() throws Exception {
        DqlSourceReadGate gate = new DqlSourceReadGate();
        TapdataEvent normalEvent = new TapdataEvent();

        gate.open();
        assertTrue(gate.allow(normalEvent));
        gate.beginPausing();
        assertFalse(gate.awaitDrained(10, java.util.concurrent.TimeUnit.MILLISECONDS));

        gate.release(normalEvent);
        assertTrue(gate.awaitDrained(100, java.util.concurrent.TimeUnit.MILLISECONDS));
    }

    @Test
    void restoresOpenStateAfterRecoveryPreparation() throws Exception {
        DqlSourceReadGate gate = new DqlSourceReadGate();
        TapdataEvent normalEvent = new TapdataEvent();

        gate.prepareForRecovery(100);
        assertEquals(DqlSourceReadGate.State.RECOVERY_ONLY, gate.getState());

        gate.restoreAfterRecovery();

        assertEquals(DqlSourceReadGate.State.OPEN, gate.getState());
        assertTrue(gate.allow(normalEvent));
    }
}
