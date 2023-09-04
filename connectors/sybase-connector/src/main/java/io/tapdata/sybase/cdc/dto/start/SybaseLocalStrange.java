package io.tapdata.sybase.cdc.dto.start;

import java.util.HashMap;
import java.util.LinkedHashMap;

public class SybaseLocalStrange implements ConfigEntity {
    Snapshot snapshot;
    Realtime realtime;

    public SybaseLocalStrange() {
        snapshot = new Snapshot();
        realtime = new Realtime();
    }

    public SybaseLocalStrange setSnapshotThreads(int threads) {
        snapshot.threads = threads;
        return this;
    }
    public SybaseLocalStrange setRealtimeThreads(int threads) {
        realtime.threads = threads;
        return this;
    }

    public SybaseLocalStrange setSnapshotTxSizeRows(int txSizeRows) {
        snapshot.txn_size_rows = txSizeRows;
        return this;
    }
    public SybaseLocalStrange setRealtimeTxSizeRows(int txSizeRows) {
        realtime.txn_size_rows = txSizeRows;
        return this;
    }
    public SybaseLocalStrange setRealtimeEncodeBinaryToBase64(boolean encodeBinaryToBase64) {
        realtime.encode_binary_to_base64 = encodeBinaryToBase64;
        return this;
    }

    @Override
    public Object toYaml() {
        HashMap<String, Object> map = new LinkedHashMap<>();
        map.put("snapshot", snapshot.toYaml());
        map.put("realtime", realtime.toYaml());
        return map;
    }


    private static class Snapshot implements ConfigEntity  {
        int threads;
        int txn_size_rows;

        @Override
        public Object toYaml() {
            HashMap<String, Object> map = new LinkedHashMap<>();
            map.put("threads", threads);
            map.put("txn-size-rows", txn_size_rows);
            return map;
        }
    }

    private static class Realtime implements ConfigEntity  {
        int threads;
        int txn_size_rows;
        boolean encode_binary_to_base64;

        @Override
        public Object toYaml() {
            HashMap<String, Object> map = new LinkedHashMap<>();
            map.put("threads", threads);
            map.put("txn-size-rows", txn_size_rows);
            map.put("encode-binary-to-base64", encode_binary_to_base64);
            return map;
        }
    }
}
