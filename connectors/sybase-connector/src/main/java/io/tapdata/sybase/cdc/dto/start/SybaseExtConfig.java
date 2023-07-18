package io.tapdata.sybase.cdc.dto.start;

import java.util.LinkedHashMap;

/**
 * @author GavinXiao
 * @description SybaseExtConfig create by Gavin
 * @create 2023/7/18 11:29
 **/
public class SybaseExtConfig implements ConfigEntity {
    Snapshot snapshot;
    Realtime realtime;

    public SybaseExtConfig() {
        snapshot = new Snapshot();
        realtime = new Realtime();
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
    }

    public Realtime getRealtime() {
        return realtime;
    }

    public void setRealtime(Realtime realtime) {
        this.realtime = realtime;
    }

    @Override
    public Object toYaml() {
        LinkedHashMap<String, Object> hashMap = new LinkedHashMap<>();
        if (null != snapshot) hashMap.put("snapshot", snapshot.toYaml());
        if (null != realtime) hashMap.put("realtime", realtime.toYaml());
        return hashMap;
    }

    public static class Snapshot implements ConfigEntity {
        int threads;//: 1
        int fetchSizeRows;//: 10_000
        boolean traceDBTasks;//: false
        int minJobSizeRows;//: 1_000_000
        int maxJobsPerChunk;//: 32

        @Override
        public Object toYaml() {
            LinkedHashMap<String, Object> hashMap = new LinkedHashMap<>();
            hashMap.put("threads", threads);
            hashMap.put("fetch-size-rows", fetchSizeRows);
            hashMap.put("_traceDBTasks", traceDBTasks);
            hashMap.put("min-job-size-rows", minJobSizeRows);
            hashMap.put("max-jobs-per-chunk", maxJobsPerChunk);
            return hashMap;
        }

        public Snapshot() {
            threads = 1;
            fetchSizeRows = 10_000;
            traceDBTasks = false;
            minJobSizeRows = 1_000_000;
            maxJobsPerChunk = 32;
        }

        public Snapshot builder() {
            return new Snapshot();
        }

        public int getThreads() {
            return threads;
        }

        public void setThreads(int threads) {
            this.threads = threads;
        }

        public int getFetchSizeRows() {
            return fetchSizeRows;
        }

        public void setFetchSizeRows(int fetchSizeRows) {
            this.fetchSizeRows = fetchSizeRows;
        }

        public boolean isTraceDBTasks() {
            return traceDBTasks;
        }

        public void setTraceDBTasks(boolean traceDBTasks) {
            this.traceDBTasks = traceDBTasks;
        }

        public int getMinJobSizeRows() {
            return minJobSizeRows;
        }

        public void setMinJobSizeRows(int minJobSizeRows) {
            this.minJobSizeRows = minJobSizeRows;
        }

        public int getMaxJobsPerChunk() {
            return maxJobsPerChunk;
        }

        public void setMaxJobsPerChunk(int maxJobsPerChunk) {
            this.maxJobsPerChunk = maxJobsPerChunk;
        }
    }

    public static class Realtime implements ConfigEntity {
        int threads;
        int fetchSizeRows;
        int fetchIntervals;
        boolean traceDBTasks;

        @Override
        public Object toYaml() {
            LinkedHashMap<String, Object> hashMap = new LinkedHashMap<>();
            hashMap.put("threads", threads);
            hashMap.put("fetch-size-rows", fetchSizeRows);
            hashMap.put("fetch-interval-s", fetchIntervals);
            hashMap.put("_traceDBTasks", traceDBTasks);
            return hashMap;
        }

        public Realtime builder() {
            return new Realtime();
        }

        public Realtime() {
            threads = 1;
            fetchIntervals = 10;
            fetchSizeRows = 100000;
            traceDBTasks = false;
        }

        public int getFetchIntervals() {
            return fetchIntervals;
        }

        public void setFetchIntervals(int fetchIntervals) {
            this.fetchIntervals = fetchIntervals;
        }

        public int getThreads() {
            return threads;
        }

        public void setThreads(int threads) {
            this.threads = threads;
        }

        public int getFetchSizeRows() {
            return fetchSizeRows;
        }

        public void setFetchSizeRows(int fetchSizeRows) {
            this.fetchSizeRows = fetchSizeRows;
        }

        public boolean isTraceDBTasks() {
            return traceDBTasks;
        }

        public void setTraceDBTasks(boolean traceDBTasks) {
            this.traceDBTasks = traceDBTasks;
        }
    }
}
