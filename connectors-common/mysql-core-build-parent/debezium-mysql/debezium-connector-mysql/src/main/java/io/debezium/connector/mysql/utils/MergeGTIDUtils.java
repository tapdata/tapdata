package io.debezium.connector.mysql.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * GTID 合并工具
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/11/2 11:30 Create
 */
public class MergeGTIDUtils {

    public enum Mode {
        OverMaxError,
        OverMaxIgnore,
        OverMaxMerge,
    }

    private final Map<String, Intervals> data = new HashMap<>();

    public MergeGTIDUtils add(String serverId, long begin, long end, Mode mode) {
        Intervals intervals = data.get(serverId);
        if (null == intervals) {
            data.put(serverId, new Intervals(begin, end));
        } else {
            intervals.add(begin, end, mode);
        }
        return this;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        for (Map.Entry<String, Intervals> en : data.entrySet()) {
            buf.append(en.getKey()).append(":").append(en.getValue().toString()).append(",");
        }
        if (!data.isEmpty())
            buf.setLength(buf.length() - 1);
        return buf.toString();
    }

    public static MergeGTIDUtils parse(String str) {
        MergeGTIDUtils ins = new MergeGTIDUtils();
        String[] uuidSets = (str == null || str.isEmpty()) ? new String[0] : str.replace("\n", "").split(",");
        for (String uuidSet : uuidSets) {
            int uuidSeparatorIndex = uuidSet.indexOf(":");
            String sourceId = uuidSet.substring(0, uuidSeparatorIndex);
            ins.data.put(sourceId, Intervals.parse(uuidSet.substring(uuidSeparatorIndex + 1)));
        }
        return ins;
    }

    public static MergeGTIDUtils merge(String basicGTIDSet, String mergeGTIDSet, Mode mode) {
        MergeGTIDUtils basic = MergeGTIDUtils.parse(basicGTIDSet);
        MergeGTIDUtils merge = MergeGTIDUtils.parse(mergeGTIDSet);

        Intervals tmp;
        for (Map.Entry<String, Intervals> en : merge.data.entrySet()) {
            tmp = en.getValue();
            do {
                basic.add(en.getKey(), tmp.start, tmp.end, mode);
                tmp = tmp.next;
            } while (null != tmp);
        }
        return basic;
    }

    private static class Intervals {
        private Intervals next;
        private long start;
        private long end;

        private Intervals(long start, long end) {
            this.start = start;
            this.end = end;
        }

        private Intervals(long start, long end, Intervals next) {
            this(start, end);
            this.next = next;
        }

        public Intervals add(long begin, long end, Mode mode) {
            // 包含参数段
            if (this.start <= begin && this.end >= end)
                return this;
            // 被参数段包含
            if (this.start >= begin && this.end <= end) {
                if (null == this.next) {
                    switch (mode) {
                        case OverMaxError:
                            throw new GTIDException("Over max: " + this.end);
                        case OverMaxIgnore:
                            this.start = begin;
                            break;
                        case OverMaxMerge:
                            this.start = begin;
                            this.end = end;
                            break;
                    }
                    return this;
                }

                this.start = begin;
                if (this.next.start > end) {
                    this.end = end;
                    return this;
                }

                // 合并后再做一次添加
                this.end = this.next.end;
                this.next = this.next.next;
                return add(begin, end, mode);
            }
            // 参数段小于开始
            if (this.start > end) {
                this.next = new Intervals(this.start, this.end, this.next);
                this.start = begin;
                this.end = end;
                return this;
            } else if (this.start == end) {
                this.start = begin;
                return this;
            }
            // 参数段大于结束
            if (this.end < begin) {
                if (null == this.next) {
                    this.next = new Intervals(begin, end);
                } else {
                    this.next.add(begin, end, mode);
                }
                return this;
            }
            // 参数段包含多段消除
            while (null != this.next && this.next.start <= end) {
                this.end = this.next.end;
                this.next = this.next.next;
            }

            if (this.end < end) {
                if (null == this.next) {
                    switch (mode) {
                        case OverMaxError:
                            throw new GTIDException("Over max: " + this.end);
                        case OverMaxIgnore:
                            break;
                        case OverMaxMerge:
                            this.end = end;
                            break;
                    }
                } else {
                    this.end = end;
                }
            }
            return this;
        }

        @Override
        public String toString() {
            return start + "-" + end + (null == next ? "" : (":" + next));
        }

        private static Intervals parse(String intervalsStr) {
            Intervals ins = null;
            String[] rawIntervals = intervalsStr.split(":");
            for (String interval : rawIntervals) {
                String[] is = interval.split("-");
                long[] split = new long[is.length];
                for (int i = 0, e = is.length; i < e; i++) {
                    split[i] = Long.parseLong(is[i]);
                }
                if (split.length == 1) {
                    split = new long[]{split[0], split[0]};
                }
                if (null == ins) {
                    ins = new Intervals(split[0], split[1]);
                } else {
                    ins.add(split[0], split[1], Mode.OverMaxMerge);
                }
            }
            return ins;
        }
    }
}
