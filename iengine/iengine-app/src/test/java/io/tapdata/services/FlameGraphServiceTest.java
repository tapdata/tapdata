package io.tapdata.services;
import org.junit.Assert;
import org.junit.Test;

public class FlameGraphServiceTest {
    @Test
    public void testFlameCPU() throws Throwable {
        // Given
        try {
            FlameGraphService flameGraphService = new FlameGraphService();
            flameGraphService.cpu();
        } catch (Throwable e) {
            Assert.fail("FlameGraphService.cpu() should not throw any exceptions");
        }
        assert true;
    }

    @Test
    public void testFlameMemory() throws Throwable {
        // Given
        try {
            FlameGraphService flameGraphService = new FlameGraphService();
            flameGraphService.memory();
        } catch (Throwable e) {
            Assert.fail("FlameGraphService.memory() should not throw any exceptions");
        }
        assert true;
    }

    @Test
    public void testFlameJstack() throws Throwable {
        // Given
        try {
            FlameGraphService flameGraphService = new FlameGraphService();
            flameGraphService.jstack();
        } catch (Throwable e) {
            Assert.fail("FlameGraphService.jstack() should not throw any exceptions");
        }
        assert true;
    }
}