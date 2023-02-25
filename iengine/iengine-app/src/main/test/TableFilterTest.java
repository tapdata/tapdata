import io.tapdata.Runnable.LoadSchemaRunner;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author samuel
 * @Description
 * @create 2022-10-12 17:46
 **/
public class TableFilterTest {
	@Test
	public void test() {
		String includeStr = "test*, aaa ";
		String excludeStr = "test1, aaa, *_bak";
		LoadSchemaRunner.TableFilter tableFilter = LoadSchemaRunner.TableFilter.create(includeStr, "");
		Assert.assertTrue(tableFilter.test("test1test"));
		Assert.assertTrue(tableFilter.test("aaa"));
		Assert.assertFalse(tableFilter.test("tst1"));
		Assert.assertFalse(tableFilter.test("aaaa"));
		Assert.assertTrue(tableFilter.test("test_bak"));
		tableFilter = LoadSchemaRunner.TableFilter.create("", excludeStr);
		Assert.assertTrue(tableFilter.test("tst1"));
		Assert.assertTrue(tableFilter.test("aaaa"));
		Assert.assertTrue(tableFilter.test("test1test"));
		Assert.assertFalse(tableFilter.test("aaa"));
		Assert.assertFalse(tableFilter.test("test_bak"));
		tableFilter = LoadSchemaRunner.TableFilter.create(includeStr, excludeStr);
		Assert.assertTrue(tableFilter.test("test1test"));
		Assert.assertFalse(tableFilter.test("aaa"));
		Assert.assertFalse(tableFilter.test("tst1"));
		Assert.assertFalse(tableFilter.test("aaaa"));
		Assert.assertFalse(tableFilter.test("test_bak"));
	}
}
