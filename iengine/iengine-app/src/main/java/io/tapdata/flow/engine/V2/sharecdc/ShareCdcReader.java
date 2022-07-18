package io.tapdata.flow.engine.V2.sharecdc;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.flow.engine.V2.sharecdc.exception.ShareCdcUnsupportedException;

import java.io.Closeable;
import java.util.function.BiConsumer;

/**
 * @author samuel
 * @Description
 * @create 2022-02-16 15:53
 **/
public interface ShareCdcReader extends Closeable {

	/**
	 * Init share cdc reader
	 *
	 * @param shareCdcContext
	 * @throws ShareCdcUnsupportedException If not available, will throw this type of exception
	 */
	void init(ShareCdcContext shareCdcContext) throws ShareCdcUnsupportedException;

	default void listen(BiConsumer<TapEvent, Object> logContentConsumer) throws Exception {
		throw new UnsupportedOperationException();
	}

	boolean isRunning();
}
