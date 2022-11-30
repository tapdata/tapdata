import org.bson.types.ObjectId;

/**
 * @author aplomb
 */
public class Test {
	public static void main(String[] args) {
		ObjectId o = new ObjectId();
		ObjectId objectId = new ObjectId(o.getTimestamp(), 0, (short)0, 0);
	}
}
