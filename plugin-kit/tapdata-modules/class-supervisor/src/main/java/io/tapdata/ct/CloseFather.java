package io.tapdata.ct;

import java.io.Closeable;
import java.io.IOException;

public abstract class CloseFather implements Closeable {
    @Override
    public void close() throws IOException {

    }
}
