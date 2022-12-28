import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author aplomb
 */
public class Test {
	public static void main(String[] args) {
		ConcurrentSkipListMap<String, String> map = new ConcurrentSkipListMap<>(String::compareTo);

		map.put("aab", "1");
		map.put("aac", "1");
		map.put("aaa", "1");
		map.put("baa", "1");
		map.put("caa", "1");

		System.out.println("keys " + map.keySet());
		String key = map.higherKey("aac1");
	}
}
