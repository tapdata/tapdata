
package io.tapdata.connector.dameng.cdc.logminer.handler;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RawTypeHandler {

    private static final Pattern HEX_TO_RAW_PATTERN = Pattern.compile("HEXTORAW\\('(.*)'\\)");

    @VisibleForTesting
    public static byte[] parseRaw(String value) throws DecoderException {
        if (value == null) {
            return null;
        }
        Matcher m = HEX_TO_RAW_PATTERN.matcher(value);
        if (m.find()) {
            return Hex.decodeHex(m.group(1).toCharArray());
        }
        return null;
    }
}
