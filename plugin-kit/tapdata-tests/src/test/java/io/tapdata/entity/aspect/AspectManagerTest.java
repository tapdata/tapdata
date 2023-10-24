package io.tapdata.entity.aspect;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.ClassFactory;
import io.tapdata.entity.utils.InstanceFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class AspectManagerTest {
    @Test
    public void testExecute() {
        AspectManager aspectManager = ClassFactory.create(AspectManager.class);
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

    @Test
    public void testExecuteAspect() {
        AspectManager aspectManager = ClassFactory.create(AspectManager.class);
//        aspectManager.registerAspect(sampleAspect);
        assertNotNull(aspectManager);
        Callable<SampleAspect> callable = () -> {
            TapTable table = new TapTable("testTable");
            SampleAspect sampleAspect = new SampleAspect().table(table);
            return sampleAspect;
        };
        AspectInterceptResult interceptResult = aspectManager.executeAspect(SampleAspect.class, callable);

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


        sampleAspectInterceptor.setIntercept(true);

        interceptResult = aspectManager.executeAspect(SampleAspect.class, callable);

        assertNotNull(interceptResult, "Should be intercepted");
        assertEquals(true, interceptResult.isIntercepted(), "Intercepted should equal true");
        assertEquals(1, sampleAspectInterceptor.getAspects().size(), "Size should be 1");
        assertEquals(1, sampleAspectInterceptor2.getAspects().size(), "Size should be 1");


        sampleAspectInterceptor.setIntercept(false);
        sampleAspectInterceptor2.setIntercept(true);

        interceptResult = aspectManager.executeAspect(SampleAspect.class, callable);

        assertNotNull(interceptResult, "Should be intercepted");
        assertEquals(true, interceptResult.isIntercepted(), "Intercepted should equal true");
        assertEquals(2, sampleAspectInterceptor.getAspects().size(), "Size should be 2");
        assertEquals(1, sampleAspectInterceptor2.getAspects().size(), "Size should be 1");


        sampleAspectInterceptor.setIntercept(false);
        sampleAspectInterceptor2.setIntercept(false);

        interceptResult = aspectManager.executeAspect(SampleAspect.class, callable);

        assertEquals(3, sampleAspectInterceptor.getAspects().size(), "Size should be 3");

        assertEquals(2, sampleAspectInterceptor2.getAspects().size(), "Size should be 2");
        assertNotNull(sampleAspectInterceptor2.getAspects().get(1).getTable(), "Table in SampleAspect should not be null");
        assertEquals("testTable", sampleAspectInterceptor2.getAspects().get(1).getTable().getName(), "Table name should be testTable");

        assertEquals(2, sampleAspectObserver.getAspects().size(), "Size should be 2");
        assertNotNull(sampleAspectObserver.getAspects().get(1).getTable(), "Table in SampleAspect should not be null");
        assertEquals("testTable", sampleAspectObserver.getAspects().get(1).getTable().getName(), "Table name should be testTable");

    }

    @Test
    void testObserverRegisterConcurrently() {
        AspectManager aspectManager = ClassFactory.create(AspectManager.class);
        assertNotNull(aspectManager);
        AtomicReference<Throwable> error = new AtomicReference<>();
        AspectObserver<SampleAspect> observer = aspect -> {

        };
        new Thread(() -> {
            for(int i = 0; i < 100; i++) {
                try {
                    aspectManager.unregisterAspectObserver(SampleAspect.class, observer);
                } catch (Throwable e) {
                    error.set(e);
                }
            }
        }).start();
        for(int i = 0; i < 1000; i++) {
            aspectManager.registerAspectObserver(SampleAspect.class, 1, observer);
        }
        assertNull(error.get(), "Should be no error occurred");
    }

    @Test
    void testInterceptorRegisterConcurrently() {
        AspectManager aspectManager = ClassFactory.create(AspectManager.class);
        assertNotNull(aspectManager);
        AspectInterceptor<SampleAspect> observer = aspect -> {
            return null;
        };
        AtomicReference<Throwable> error = new AtomicReference<>();
        new Thread(() -> {
            for(int i = 0; i < 100; i++) {
                try {
                    aspectManager.unregisterAspectInterceptor(SampleAspect.class, observer);
                } catch (Throwable e) {
                    error.set(e);
                }
            }
        }).start();
        for(int i = 0; i < 1000; i++) {
            aspectManager.registerAspectInterceptor(SampleAspect.class, 1, observer);
        }
        assertNull(error.get(), "Should be no error occurred");
    }

    @Test
    void testObserverExecuteUnregisterConcurrently() {
        AspectManager aspectManager = ClassFactory.create(AspectManager.class);
        assertNotNull(aspectManager);
        AspectObserver<SampleAspect> observer = aspect -> {

        };
        for(int i = 0; i < 1000; i++) {
            aspectManager.registerAspectObserver(SampleAspect.class, 1, observer);
        }
        AtomicReference<Throwable> error = new AtomicReference<>();
        new Thread(() -> {
            for(int i = 0; i < 100; i++) {
                try {
                    aspectManager.unregisterAspectObserver(SampleAspect.class, observer);
                } catch (Throwable e) {
                    error.set(e);
                }
            }
        }).start();
        aspectManager.executeAspectObservers(new SampleAspect());
        assertNull(error.get(), "Should be no error occurred");
    }

    @Test
    void testInterceptorExecuteUnregisterConcurrently() {
        AspectManager aspectManager = ClassFactory.create(AspectManager.class);
        assertNotNull(aspectManager);
        AspectInterceptor<SampleAspect> observer = aspect -> {
            return null;
        };
        for(int i = 0; i < 1000; i++) {
            aspectManager.registerAspectInterceptor(SampleAspect.class, 1, observer);
        }
        AtomicReference<Throwable> error = new AtomicReference<>();
        new Thread(() -> {
            for(int i = 0; i < 100; i++) {
                try {
                    aspectManager.unregisterAspectInterceptor(SampleAspect.class, observer);
                } catch (Throwable e) {
                    error.set(e);
                }
            }
        }).start();
        aspectManager.executeAspectInterceptors(new SampleAspect());
        assertNull(error.get(), "Should be no error occurred");
    }

    @Test
    public void testRegisterAtRuntime() {
        AspectManager aspectManager = ClassFactory.create(AspectManager.class);
        assertNotNull(aspectManager);
        List<SampleAspect> observerAspects = new ArrayList<>();
        AspectObserver<SampleAspect> observer = aspect -> {
            observerAspects.add(aspect);
        };
        List<SampleAspect> interceptorAspects = new ArrayList<>();
        AspectInterceptor<SampleAspect> interceptor = aspect -> {
            interceptorAspects.add(aspect);
            return null;
        };
        aspectManager.registerAspectObserver(SampleAspect.class, 1, observer);
        aspectManager.registerAspectInterceptor(SampleAspect.class, 1, interceptor);
//        aspectManager.registerAspect(sampleAspect);
        TapTable table = new TapTable("testTable");
        SampleAspect sampleAspect = new SampleAspect().table(table);
        AspectInterceptResult interceptResult = aspectManager.executeAspectInterceptors(sampleAspect);
        aspectManager.executeAspectObservers(sampleAspect);

        assertEquals(1, observerAspects.size(), "Size should be 1");
        assertNotNull(observerAspects.get(0).getTable(), "Table in SampleAspect should not be null");
        assertEquals("testTable", observerAspects.get(0).getTable().getName(), "Table name should be testTable");

        assertEquals(1, interceptorAspects.size(), "Size should be 1");
        assertNotNull(interceptorAspects.get(0).getTable(), "Table in SampleAspect should not be null");
        assertEquals("testTable", interceptorAspects.get(0).getTable().getName(), "Table name should be testTable");

        SampleAspectInterceptor2 sampleAspectInterceptor2 = aspectManager.getAspectInterceptor(SampleAspectInterceptor2.class);
        assertNotNull(sampleAspectInterceptor2, "SampleAspectInterceptor2 should not be null");
        assertNotNull(sampleAspectInterceptor2.getAspects(), "Aspects should not be null");
        assertEquals(1, sampleAspectInterceptor2.getAspects().size(), "Size should be 1");
        assertNotNull(sampleAspectInterceptor2.getAspects().get(0).getTable(), "Table in SampleAspect should not be null");
        assertEquals("testTable", sampleAspectInterceptor2.getAspects().get(0).getTable().getName(), "Table name should be testTable");
    }

    @Test
    public void testUnregisterAtRuntime() {
        AspectManager aspectManager = ClassFactory.create(AspectManager.class);
        assertNotNull(aspectManager);
        AspectObserver<SampleAspect> observer = aspect -> {
        };
        AspectInterceptor<SampleAspect> interceptor = aspect -> {
            return null;
        };
        aspectManager.registerAspectObserver(SampleAspect.class, 1, observer);
        aspectManager.registerAspectInterceptor(SampleAspect.class, 1, interceptor);

        TapTable table = new TapTable("testTable");
        SampleAspect sampleAspect = new SampleAspect().table(table);
        AspectInterceptResult interceptResult = aspectManager.executeAspectInterceptors(sampleAspect);
        aspectManager.executeAspectObservers(sampleAspect);

        assertNotNull(aspectManager.getAspectObserver(observer.getClass()));
        assertNotNull(aspectManager.getAspectInterceptor(interceptor.getClass()));

        aspectManager.unregisterAspectObserver(SampleAspect.class, observer);
        assertNull(aspectManager.getAspectObserver(observer.getClass()));

        aspectManager.unregisterAspectInterceptor(SampleAspect.class, interceptor);
        assertNull(aspectManager.getAspectInterceptor(interceptor.getClass()));

        assertNotNull(aspectManager.getAspectInterceptor(SampleAspectInterceptor2.class));

        aspectManager.unregisterAspectInterceptor(SampleAspect.class, SampleAspectInterceptor2.class);
        assertNull(aspectManager.getAspectInterceptor(SampleAspectInterceptor2.class));
    }

    @Test
    public void testNewRegisterAtRuntime() {
        AspectManager aspectManager = ClassFactory.create(AspectManager.class);
        assertNotNull(aspectManager);
        AspectObserver<EmptyAspect> observer = aspect -> {
        };
        AspectInterceptor<EmptyAspect> interceptor = aspect -> {
            return null;
        };

        AtomicLong counter = new AtomicLong();
        AspectInterceptResult interceptResult = aspectManager.executeAspect(EmptyAspect.class, () -> {
            counter.incrementAndGet();
            return new EmptyAspect();
        });

        assertEquals(0, counter.get());

        aspectManager.registerAspectObserver(EmptyAspect.class, 1, observer);
        aspectManager.registerAspectInterceptor(EmptyAspect.class, 1, interceptor);

        interceptResult = aspectManager.executeAspect(EmptyAspect.class, () -> {
            counter.incrementAndGet();
            return new EmptyAspect();
        });

        assertEquals(1, counter.get());

    }

    @Test
    public void testIgnoreErrors() {
        AspectManager aspectManager = ClassFactory.create(AspectManager.class);
        assertNotNull(aspectManager);
        AspectObserver<EmptyAspect> observer = aspect -> {
            throw new NullPointerException();
        };
        AspectInterceptor<EmptyAspect> interceptor = aspect -> {
            throw new NullPointerException();
        };

        aspectManager.registerAspectObserver(EmptyAspect.class, 1, observer, true);
        aspectManager.registerAspectInterceptor(EmptyAspect.class, 1, interceptor, true);

        aspectManager.executeAspect(EmptyAspect.class, () -> {
            return new EmptyAspect();
        });
        assertTrue(true);

        aspectManager.unregisterAspectObserver(EmptyAspect.class, observer);
        aspectManager.unregisterAspectInterceptor(EmptyAspect.class, interceptor);

        aspectManager.registerAspectObserver(EmptyAspect.class, 1, observer, false);

        try {
            aspectManager.executeAspect(EmptyAspect.class, () -> {
                return new EmptyAspect();
            });
            fail();
        } catch (Throwable throwable) {
            assertTrue(true);
        }
        aspectManager.unregisterAspectObserver(EmptyAspect.class, observer);


        aspectManager.registerAspectInterceptor(EmptyAspect.class, 1, interceptor, false);
        try {
            aspectManager.executeAspect(EmptyAspect.class, () -> {
                return new EmptyAspect();
            });
            fail();
        } catch (Throwable throwable) {
            assertTrue(true);
        }

    }

    public static void main(String[] args) {
        AspectManager aspectManager = InstanceFactory.instance(AspectManager.class);
        for(int i = 0; i < 100; i++)
            aspectManager.executeAspect(new EmptyAspect());

//        aspectManager.registerAspectObserver(EmptyAspect.class, 1, new AspectObserver<EmptyAspect>() {
//            @Override
//            public void observe(EmptyAspect aspect) {
//
//            }
//        });

        long time = System.currentTimeMillis();
        for(int i = 0; i < 1000000; i++)
            aspectManager.executeAspect(new EmptyAspect());
        System.out.println("takes " + (System.currentTimeMillis() - time));
        //takes 15



        time = System.currentTimeMillis();
        for(int i = 0; i < 1000000; i++)
            aspectManager.executeAspect(EmptyAspect.class, EmptyAspect::new);
        System.out.println("callable takes " + (System.currentTimeMillis() - time));
        //callable takes 10
    }
}
