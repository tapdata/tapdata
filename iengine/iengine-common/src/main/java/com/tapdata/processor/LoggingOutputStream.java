package com.tapdata.processor;

import io.tapdata.entity.logger.Log;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LoggingOutputStream extends OutputStream {
	private static final Logger LOGGER = LogManager.getLogger(LoggingOutputStream.class);
	public static final int DEFAULT_BUFFER_SIZE = 4096;
	private static final Map<Level, MethodHandle> LOG_METHODS = new ConcurrentHashMap<>();
	private final MethodHandle handle;
	private final boolean twoByteLineSeparator;
	private final byte eol;
	private final byte eolLeader;
	private volatile boolean closed = false;
	private byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
	private int bufferSize = DEFAULT_BUFFER_SIZE;
	private int byteCount = 0;
	private boolean haveLeader = false;

	public LoggingOutputStream(Log logger, Level level) {
		try {
			this.handle = ((MethodHandle) LOG_METHODS.computeIfAbsent(level, LoggingOutputStream::getHandle)).bindTo(logger);
		} catch (AssertionError var4) {
			LOGGER.error("Failed to create " + LoggingOutputStream.class.getSimpleName() + " instance for Logger \"" + logger.getClass().getName() + "[" + level + "]\"", var4);
			throw var4;
		}

		String lineSeparator = System.lineSeparator();
		this.twoByteLineSeparator = lineSeparator.length() == 2;
		this.eol = (byte) lineSeparator.charAt(lineSeparator.length() - 1);
		this.eolLeader = (byte) (this.twoByteLineSeparator ? lineSeparator.charAt(0) : 0);
	}

	public void write(int b) throws IOException {
		synchronized (this) {
			this.checkOpen();
			if (this.twoByteLineSeparator && (byte) b == this.eolLeader) {
				this.haveLeader = true;
			} else {
				if ((byte) b == this.eol) {
					this.haveLeader = false;
					this.flushInternal();
					return;
				}

				if (this.haveLeader) {
					this.appendByte(this.eolLeader);
					this.haveLeader = false;
				}

				this.appendByte((byte) b);
			}

		}
	}

	public void flush() throws IOException {
		synchronized (this) {
			this.checkOpen();
			this.flushInternal();
		}
	}

	public void close() throws IOException {
		synchronized (this) {
			this.flushInternal();
			this.closed = true;
		}
	}

	private void flushInternal() throws IOException {
		if (this.haveLeader) {
			this.appendByte(this.eolLeader);
			this.haveLeader = false;
		}

		if (this.byteCount != 0) {
			this.log(new String(this.buffer, 0, this.byteCount, StandardCharsets.UTF_8));
			this.byteCount = 0;
		}
	}

	private void log(String line) throws IOException {
		try {
			this.handle.invokeExact(line);
		} catch (Error | RuntimeException var3) {
			throw var3;
		} catch (Throwable var4) {
			throw new IOException(String.format("Unexpected error calling %s: %s", this.handle, var4), var4);
		}
	}

	private void appendByte(byte b) {
		if (this.byteCount == this.bufferSize) {
			int newBufferSize = this.bufferSize + DEFAULT_BUFFER_SIZE;
			byte[] newBuffer = new byte[newBufferSize];
			System.arraycopy(this.buffer, 0, newBuffer, 0, this.byteCount);
			this.buffer = newBuffer;
			this.bufferSize = newBufferSize;
		}

		this.buffer[this.byteCount++] = b;
	}

	private void checkOpen() throws IOException {
		if (this.closed) {
			throw new IOException("stream closed");
		}
	}

	private static MethodHandle getHandle(Level level) {
		String methodName = level.name().toLowerCase(Locale.ROOT);

		try {
			return MethodHandles.publicLookup().findVirtual(Log.class, methodName, MethodType.methodType(Void.TYPE, String.class));
		} catch (IllegalAccessException | NoSuchMethodException var3) {
			throw new AssertionError(var3);
		}
	}
}
