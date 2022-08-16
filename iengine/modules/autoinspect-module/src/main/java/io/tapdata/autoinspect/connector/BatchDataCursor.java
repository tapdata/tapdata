package io.tapdata.autoinspect.connector;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/9 10:43 Create
 */
public abstract class BatchDataCursor<T> implements IDataCursor<T> {

    private int offset;
    private final int limit;
    private IDataCursor<T> cursor;

    public BatchDataCursor(int offset, int limit) throws Exception {
        this.offset = offset;
        this.limit = limit;

        //Initialize cursor the first time the next function is called
        this.cursor = new IDataCursor<T>() {
            private IDataCursor<T> firstCursor;

            @Override
            public T next() throws Exception {
                firstCursor = batchCursor(offset, limit);
                cursor = firstCursor;
                if (null == firstCursor) {
                    return null;
                }
                return firstCursor.next();
            }

            @Override
            public void close() throws Exception {
                firstCursor.close();
            }
        };
    }

    @Override
    public T next() throws Exception {
        if (null == cursor) return null;

        T o = cursor.next();
        if (null == o) {
            close();
            cursor = batchCursor(nextPageOffset(), limit);
            if (null == cursor) return null;

            o = cursor.next();
            if (null == o) {
                close();
            }
        }
        return o;
    }

    @Override
    public void close() throws Exception {
        if (null != cursor) {
            cursor.close();
            cursor = null;
        }
    }

    private int nextPageOffset() {
        return offset += limit;
    }

    protected abstract IDataCursor<T> batchCursor(int offset, int limit) throws Exception;

}
