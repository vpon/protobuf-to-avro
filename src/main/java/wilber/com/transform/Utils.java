package wilber.com.transform;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema.Type;
import org.apache.avro.Schema;

import com.squareup.protoparser.EnumType;

public class Utils {

	static public final Map<String, Schema.Type> ProtoBufPrimitiveTypeMap = new HashMap<String, Schema.Type>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 4300028261621141009L;

		{
			put("double", Type.DOUBLE);
			put("float", Type.FLOAT);
			put("int32", Type.INT);
			put("int64", Type.LONG);
			put("uint32", Type.INT);
			put("uint64", Type.LONG);
			put("sint32", Type.INT);
			put("sint64", Type.LONG);
			put("fixed32", Type.INT);
			put("fix64", Type.LONG);
			put("sfix32", Type.INT);
			put("sfix64", Type.LONG);
			put("bool", Type.BOOLEAN);
			put("string", Type.STRING);
			put("bytes", Type.BYTES);
		}
	};
	
	public static String getProtoDefault(Object defaultValue) {
		String defaultString = null;

		if (defaultValue instanceof EnumType.Value) {
			defaultString = ((EnumType.Value) defaultValue).getName();
		} else {
			defaultString = (String) defaultValue;
		}
		return defaultString;
	}
}
