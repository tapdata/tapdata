package com.tapdata.constant;

import org.junit.jupiter.api.Test;

public class StringCompressionTest {

    @Test
    void testCompress() throws Exception {
        String str = "Hello, World!";
        String compressed = StringCompression.compress(str);
        String decompressed = StringCompression.uncompress(compressed);
        assert str.equals(decompressed);
    }

    @Test
    void testCompressV2() throws Exception {
        String str = "Hello, World!";
        String compressed = StringCompression.compressV2(str);
        String decompressed = StringCompression.uncompressV2(compressed);
        assert str.equals(decompressed);
    }

    @Test
    void testCompressChinese() throws Exception {
        String str = "你好，世界";
        String compressed = StringCompression.compress(str);
        String decompressed = StringCompression.uncompress(compressed);
        assert !str.equals(decompressed);
    }

    @Test
    void testCompressChineseV2() throws Exception {
        String str = "你好，世界";
        String compressed = StringCompression.compressV2(str);
        String decompressed = StringCompression.uncompressV2(compressed);
        assert str.equals(decompressed);
    }
}
