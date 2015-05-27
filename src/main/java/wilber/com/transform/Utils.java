package wilber.com.transform;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema.Type;
import org.apache.avro.Schema;

import com.squareup.protoparser.EnumType;

public class Utils {
	
	static public final String BROTOBUF_OPTIONAL = "optional";
	static public final String BROTOBUF_REPEATED = "repeated";

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

	static public final Map<com.google.protobuf.Descriptors.FieldDescriptor.Type, org.apache.avro.Schema.Type> ProtoBufPrimitiveTypeMapAvroType = new HashMap<com.google.protobuf.Descriptors.FieldDescriptor.Type, org.apache.avro.Schema.Type>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		{
			put(com.google.protobuf.Descriptors.FieldDescriptor.Type.DOUBLE,
					org.apache.avro.Schema.Type.DOUBLE);
			put(com.google.protobuf.Descriptors.FieldDescriptor.Type.FLOAT,
					org.apache.avro.Schema.Type.FLOAT);
			put(com.google.protobuf.Descriptors.FieldDescriptor.Type.INT32,
					org.apache.avro.Schema.Type.INT);
			put(com.google.protobuf.Descriptors.FieldDescriptor.Type.INT64,
					org.apache.avro.Schema.Type.LONG);
			put(com.google.protobuf.Descriptors.FieldDescriptor.Type.UINT32,
					org.apache.avro.Schema.Type.INT);
			put(com.google.protobuf.Descriptors.FieldDescriptor.Type.UINT64,
					org.apache.avro.Schema.Type.LONG);
			put(com.google.protobuf.Descriptors.FieldDescriptor.Type.SINT32,
					org.apache.avro.Schema.Type.INT);
			put(com.google.protobuf.Descriptors.FieldDescriptor.Type.SINT64,
					org.apache.avro.Schema.Type.LONG);
			put(com.google.protobuf.Descriptors.FieldDescriptor.Type.FIXED32,
					org.apache.avro.Schema.Type.INT);
			put(com.google.protobuf.Descriptors.FieldDescriptor.Type.FIXED64,
					org.apache.avro.Schema.Type.LONG);
			put(com.google.protobuf.Descriptors.FieldDescriptor.Type.SFIXED32,
					org.apache.avro.Schema.Type.INT);
			put(com.google.protobuf.Descriptors.FieldDescriptor.Type.SFIXED64,
					org.apache.avro.Schema.Type.LONG);
			put(com.google.protobuf.Descriptors.FieldDescriptor.Type.BOOL,
					org.apache.avro.Schema.Type.BOOLEAN);
			put(com.google.protobuf.Descriptors.FieldDescriptor.Type.STRING,
					org.apache.avro.Schema.Type.STRING);
			put(com.google.protobuf.Descriptors.FieldDescriptor.Type.BYTES,
					org.apache.avro.Schema.Type.BYTES);
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
