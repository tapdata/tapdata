package tests;

import io.tapdata.file.TapFileStorage;
import io.tapdata.file.TapFileStorageBuilder;
import io.tapdata.storage.local.LocalFileStorage;
import org.junit.jupiter.api.Test;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class Main {
	@Test
	public void test() {
		TapFileStorage storage = new TapFileStorageBuilder()
				.withClassLoader(LocalFileStorage.class.getClassLoader())
				.withParams(map(entry("rootPath", "/root/")))
				.withStorageClassName(LocalFileStorage.class.getName())
				.build();
		assertNotNull(storage);
	}
}
