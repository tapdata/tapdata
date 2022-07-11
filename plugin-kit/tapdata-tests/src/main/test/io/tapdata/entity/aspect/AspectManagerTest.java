package io.tapdata.entity.aspect;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AspectManagerTest {
    @Test
    public void testAspectManager() {
        AspectManager aspectManager = InstanceFactory.instance(AspectManager.class);
//        aspectManager.registerAspect(sampleAspect);
        TapTable table = new TapTable("testTable");
        SampleAspect sampleAspect = new SampleAspect().table(table);
        AspectInterceptResult interceptResult = aspectManager.executeAspectInterceptors(sampleAspect);
        aspectManager.executeAspectObservers(sampleAspect);

        SampleAspectObserver sampleAspectObserver = aspectManager.getAspectObserver(SampleAspectObserver.class);
        assertNotNull(sampleAspectObserver, "sampleAspectObserver should not be null");
        assertNotNull(sampleAspectObserver.getAspects(), "observedSampleAspects should not be null");
        assertEquals(1, sampleAspectObserver.getAspects().size(), "Size should be 1");
        assertNotNull(sampleAspectObserver.getAspects().get(0).getTable(), "Table in SampleAspect should not be null");
        assertEquals("testTable", sampleAspectObserver.getAspects().get(0).getTable().getName(), "Table name should be testTable");

        SampleAspectInterceptor sampleAspectInterceptor = aspectManager.getAspectInterceptor(SampleAspectInterceptor.class);
        assertNotNull(sampleAspectInterceptor, "SampleAspectInterceptor should not be null");
        assertNotNull(sampleAspectInterceptor.getAspects(), "Aspects should not be null");
        assertEquals(1, sampleAspectInterceptor.getAspects().size(), "Size should be 1");
        assertNotNull(sampleAspectInterceptor.getAspects().get(0).getTable(), "Table in SampleAspect should not be null");
        assertEquals("testTable", sampleAspectInterceptor.getAspects().get(0).getTable().getName(), "Table name should be testTable");

        SampleAspectInterceptor2 sampleAspectInterceptor2 = aspectManager.getAspectInterceptor(SampleAspectInterceptor2.class);
        assertNotNull(sampleAspectInterceptor2, "SampleAspectInterceptor2 should not be null");
        assertNotNull(sampleAspectInterceptor2.getAspects(), "Aspects should not be null");
        assertEquals(1, sampleAspectInterceptor2.getAspects().size(), "Size should be 1");
        assertNotNull(sampleAspectInterceptor2.getAspects().get(0).getTable(), "Table in SampleAspect should not be null");
        assertEquals("testTable", sampleAspectInterceptor2.getAspects().get(0).getTable().getName(), "Table name should be testTable");


        sampleAspect = new SampleAspect().table(table);
        sampleAspectInterceptor.setIntercept(true);

        interceptResult = aspectManager.executeAspectInterceptors(sampleAspect);

        assertNotNull(interceptResult, "Should be intercepted");
        assertEquals(true, interceptResult.isIntercepted(), "Intercepted should equal true");
        assertEquals(1, sampleAspectInterceptor.getAspects().size(), "Size should be 1");
        assertEquals(1, sampleAspectInterceptor2.getAspects().size(), "Size should be 1");


        sampleAspectInterceptor.setIntercept(false);
        sampleAspectInterceptor2.setIntercept(true);

        interceptResult = aspectManager.executeAspectInterceptors(sampleAspect);

        assertNotNull(interceptResult, "Should be intercepted");
        assertEquals(true, interceptResult.isIntercepted(), "Intercepted should equal true");
        assertEquals(2, sampleAspectInterceptor.getAspects().size(), "Size should be 2");
        assertEquals(1, sampleAspectInterceptor2.getAspects().size(), "Size should be 1");


        sampleAspectInterceptor.setIntercept(false);
        sampleAspectInterceptor2.setIntercept(false);

        interceptResult = aspectManager.executeAspectInterceptors(sampleAspect);
        if(interceptResult == null || !interceptResult.isIntercepted())
            aspectManager.executeAspectObservers(sampleAspect);

        assertEquals(3, sampleAspectInterceptor.getAspects().size(), "Size should be 3");

        assertEquals(2, sampleAspectInterceptor2.getAspects().size(), "Size should be 2");
        assertNotNull(sampleAspectInterceptor2.getAspects().get(1).getTable(), "Table in SampleAspect should not be null");
        assertEquals("testTable", sampleAspectInterceptor2.getAspects().get(1).getTable().getName(), "Table name should be testTable");

        assertEquals(2, sampleAspectObserver.getAspects().size(), "Size should be 2");
        assertNotNull(sampleAspectObserver.getAspects().get(1).getTable(), "Table in SampleAspect should not be null");
        assertEquals("testTable", sampleAspectObserver.getAspects().get(1).getTable().getName(), "Table name should be testTable");

    }

    public static void main(String[] args) {
        AspectManager aspectManager = InstanceFactory.instance(AspectManager.class);
        long time = System.currentTimeMillis();
        for(int i = 0; i < 1000000; i++)
            aspectManager.executeAspectObservers(new PerformanceAspect());
        System.out.println("takes " + (System.currentTimeMillis() - time));
        //takes 31
    }
}
