package com.tapdata.tm.init;

import lombok.NonNull;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/16 15:40 Create
 */
public class PatchVersion implements Comparable<PatchVersion> {

    private final int v1;
    private final int v2;
    private final int seq;

    public PatchVersion(int v1, int v2, int seq) {
        this.v1 = v1;
        this.v2 = v2;
        this.seq = seq;
    }

    /**
     * compare of ignore ordinal
     *
     * @param o compare version
     * @return compare result
     */
    public int compareVersion(@NonNull PatchVersion o) {
        int v = v1 - o.v1;
        if (0 == v) {
            v = v2 - o.v2;
        }
        return v;
    }

    @Override
    public int compareTo(@NonNull PatchVersion o) {
        int v = compareVersion(o);
        if (0 == v) {
            v = seq - o.seq;
        }
        return v;
    }

    @Override
    public String toString() {
        return String.format("%d.%d-%d", v1, v2, seq);
    }

    /**
     * Parse version by string
     *
     * @param version version string
     * @return version instance
     */
    public static PatchVersion valueOf(@NonNull String version) {
        Pattern p = Pattern.compile("([0-9]+)(\\.([0-9]+))?(-([0-9]+))?");
        Matcher m = p.matcher(version);
        if (m.find()) {
            return new PatchVersion(
                    Optional.ofNullable(m.group(1)).map(Integer::parseInt).orElse(0)
                    , Optional.ofNullable(m.group(3)).map(Integer::parseInt).orElse(0)
                    , Optional.ofNullable(m.group(5)).map(Integer::parseInt).orElse(0)
            );
        }
        throw new RuntimeException("Illegal format, patch version: " + version);
    }
}
