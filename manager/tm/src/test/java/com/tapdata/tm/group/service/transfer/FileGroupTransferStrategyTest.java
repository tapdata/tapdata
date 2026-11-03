package com.tapdata.tm.group.service.transfer;

import com.tapdata.tm.group.constant.GroupConstants;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockMultipartFile;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * ES-3：包自带的脱敏标记（[ADR-0034] D3）是根级 sidecar，必须像 vault.json 一样**按原样**读回。
 *
 * 走通用 JSON 分支的话，{@code parseTaskUpAndLoadList} 会拿它当资源列表解析、失败后静默返回空列表——
 * 标记就此消失，导入侧只能退回「按老包处理」，D4 的兼容口径也就永远命中，等于 ES-3 没做。
 */
class FileGroupTransferStrategyTest {

    private final FileGroupTransferStrategy strategy = new FileGroupTransferStrategy();

    private MockMultipartFile tarOf(Map<String, String> entries) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (TarArchiveOutputStream taos = new TarArchiveOutputStream(baos)) {
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                byte[] bytes = entry.getValue().getBytes(StandardCharsets.UTF_8);
                TarArchiveEntry tarEntry = new TarArchiveEntry(entry.getKey());
                tarEntry.setSize(bytes.length);
                taos.putArchiveEntry(tarEntry);
                taos.write(bytes);
                taos.closeArchiveEntry();
            }
            taos.finish();
        }
        return new MockMultipartFile("file", "group.tar", null, baos.toByteArray());
    }

    @Nested
    @DisplayName("包脱敏标记的读回")
    class PackageManifestTest {

        private static final String MANIFEST_JSON = "{\"secretsMasked\":false}";

        @Test
        @DisplayName("标记文件按原样读回，不被当成资源列表解析掉")
        void manifestIsReadBackVerbatim() throws Exception {
            Map<String, List<TaskUpAndLoadDto>> payloads = strategy.parseImportPayloads(
                    tarOf(Map.of(GroupConstants.PACKAGE_MANIFEST_FILE, MANIFEST_JSON)));

            List<TaskUpAndLoadDto> items = payloads.get(GroupConstants.PACKAGE_MANIFEST_FILE);
            assertNotNull(items, "标记文件必须能按自己的文件名取到——导入侧靠它区分「真的没配」和「被抹空」");
            assertEquals(1, items.size());
            assertEquals(MANIFEST_JSON, items.get(0).getJson(),
                    "必须是原始 JSON：走通用分支会被解析成空的资源列表，标记静默消失");
        }

        @Test
        @DisplayName("标记文件不混进资源 payload——它不是要被导入的文档")
        void manifestIsNotTreatedAsAResource() throws Exception {
            Map<String, List<TaskUpAndLoadDto>> payloads = strategy.parseImportPayloads(
                    tarOf(new LinkedHashMap<>(Map.of(
                            GroupConstants.PACKAGE_MANIFEST_FILE, MANIFEST_JSON,
                            "GroupInfo.json", "[]"))));

            assertFalse(payloads.getOrDefault("GroupInfo.json", List.of()).stream()
                            .anyMatch(item -> MANIFEST_JSON.equals(item.getJson())),
                    "标记是包的元信息，不能被当作资源导进库（D3：不写进任何被导入的文档）");
        }

        @Test
        @DisplayName("老包没有标记文件：payloads 里就没有这个 key，读侧自行按兼容口径处理")
        void oldPackageHasNoManifestKey() throws Exception {
            Map<String, List<TaskUpAndLoadDto>> payloads = strategy.parseImportPayloads(
                    tarOf(Map.of("GroupInfo.json", "[]")));

            assertFalse(payloads.containsKey(GroupConstants.PACKAGE_MANIFEST_FILE),
                    "老包本来就没有这个文件，读侧不该凭空造一个 key 出来");
        }
    }
}
