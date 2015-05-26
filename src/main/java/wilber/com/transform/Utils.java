package wilber.com.transform;

import java.util.HashMap;
import java.util.Map;

public class Utils {

	static public final Map<String, String> ProtoBufPrimitiveTypeMap = new HashMap<String, String>() {
		{
			put("double", "double");
			put("float", "float");
			put("int32", "int");
			put("int64", "long");
			put("uint32", "int");
			put("uint64", "long");
			put("sint32", "int");
			put("sint64", "long");
			put("fixed32", "int");
			put("fix64", "long");
			put("sfix32", "ing");
			put("sfix64", "long");
			put("bool", "boolean");
			put("string", "string");
			put("bytes", "bytes");
		}
	};
}
