package com.tapdata.processor.standard;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.script.Invocable;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AcceptClassLoaderTest {
    AcceptClassLoader acceptClassLoader;

    @Test
    void testDoAccept() {
        acceptClassLoader = mock(AcceptClassLoader.class);
        when(acceptClassLoader.before(any(ClassLoader[].class))).thenReturn("");
        when(acceptClassLoader.after(anyString(), any(ClassLoader[].class))).thenReturn(mock(Invocable.class));
        when(acceptClassLoader.doAccept()).thenCallRealMethod();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Assertions.assertDoesNotThrow(() -> acceptClassLoader.doAccept());
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }
    @Test
    void testDoAcceptExternalClassLoaderNotNull() {
        acceptClassLoader = mock(AcceptClassLoader.class);
        when(acceptClassLoader.before(any(ClassLoader[].class))).then(a -> {
            ClassLoader[] argument = a.getArgument(0, ClassLoader[].class);
            argument[0] = mock(ClassLoader.class);
            return null;
        });
        when(acceptClassLoader.after(anyString(), any(ClassLoader[].class))).thenReturn(mock(Invocable.class));
        when(acceptClassLoader.doAccept()).thenCallRealMethod();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Assertions.assertDoesNotThrow(() -> acceptClassLoader.doAccept());
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    @Test
    void testDoAcceptCurrentThreadNoClassLoader() {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        acceptClassLoader = mock(AcceptClassLoader.class);
        when(acceptClassLoader.before(any(ClassLoader[].class))).then(a -> {
            Thread.currentThread().setContextClassLoader(null);
            return null;
        });
        when(acceptClassLoader.after(anyString(), any(ClassLoader[].class))).thenReturn(mock(Invocable.class));
        when(acceptClassLoader.doAccept()).thenCallRealMethod();
        try {
            Assertions.assertDoesNotThrow(() -> acceptClassLoader.doAccept());
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }
}