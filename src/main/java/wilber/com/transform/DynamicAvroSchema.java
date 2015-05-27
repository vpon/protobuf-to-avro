package wilber.com.transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import com.squareup.protoparser.EnumType;
import com.squareup.protoparser.MessageType;
import com.squareup.protoparser.ProtoFile;
import com.squareup.protoparser.Type;

public class DynamicAvroSchema {

	private Map<String, Schema> avroSchemaMap = new HashMap<String, Schema>();
	private String rootPackageName = null;
	private String avroDoc = null;
	private ProtoFile protoFile = null;

	public void buildSchemaByProtoSchema(ProtoFile protoFile) {
		rootPackageName = protoFile.getPackageName();
		this.protoFile = protoFile;
		buildSchemaMap(protoFile);
	}

	public Map<String, Schema> getAvroSchemaMap() {
		return avroSchemaMap;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (avroSchemaMap.size() > 0) {
			sb.append("AVRO Schema:\n");
			for (Map.Entry entry : avroSchemaMap.entrySet()) {
				sb.append("Schema name: " + entry.getKey()
						+ "\nSchema structure: " + entry.getValue() + "\n");
			}
		} else
			sb.append("you never build any avro schema");
		return sb.toString();
	}

	private void buildSchemaMap(ProtoFile protoFile) {
		if (!protoFile.getTypes().isEmpty())
			for (com.squareup.protoparser.Type type : protoFile.getTypes())
				if (!avroSchemaMap.containsKey(type.getName())) {
					if (type instanceof MessageType) {
						MessageType msgType = (MessageType) type;
						Schema rSchema = getRecordSchema(msgType, null);
						avroSchemaMap.put(rSchema.getName(), rSchema);
					}
					if (type instanceof EnumType) {
						EnumType enumType = (EnumType) type;
						Schema eSchema = getEnumSchema(enumType, null);
						avroSchemaMap.put(eSchema.getName(), eSchema);
					}
				}
	}

	private Schema getRecordSchema(MessageType msgType, String parentName) {
		String packageName = getPackageName(parentName);
		Schema rs = Schema.createRecord(msgType.getName(), avroDoc,
				packageName, false);
		// Prebuild nested schema
		for (Type nestedType : msgType.getNestedTypes()) {
			if (nestedType instanceof MessageType) {
				MessageType nestedMsgType = (MessageType) nestedType;
				Schema rSchema = getRecordSchema(nestedMsgType,
						msgType.getName());
				avroSchemaMap.put(rSchema.getName(), rSchema);
			} else if (nestedType instanceof EnumType) {
				EnumType nestedEnumType = (EnumType) nestedType;
				Schema eSchema = getEnumSchema(nestedEnumType,
						msgType.getName());
				avroSchemaMap.put(eSchema.getName(), eSchema);
			}
		}

		List<Field> fields = new ArrayList<Field>();
		for (MessageType.Field field : msgType.getFields()) {
			String fieldName = field.getName();
			String fieldType = field.getType();
			Schema fieldSchema = null;
			if (Utils.ProtoBufPrimitiveTypeMap.containsKey(fieldType)) {
				fieldSchema = Schema.create(Utils.ProtoBufPrimitiveTypeMap
						.get(fieldType));
			} else {
				fieldSchema = getProperSchema(fieldType);
			}
			if (field.getLabel().toString().toLowerCase()
					.equals(Utils.BROTOBUF_REPEATED)) {
				// protobuf repeat correspond to avro array
				fieldSchema = Schema.createArray(fieldSchema);
			}
			if (field.getLabel().toString().toLowerCase()
					.equals(Utils.BROTOBUF_OPTIONAL)) {
				// protobuf optional correspond to avro union with null
				fieldSchema = getOptionSchema(fieldSchema);
			}
			fields.add(new Field(fieldName, fieldSchema, avroDoc, null));
		}
		rs.setFields(fields);
		return rs;
	}

	private Schema getProperSchema(String schemaName) {
		Schema rs = null;
		if (avroSchemaMap.containsKey(schemaName))
			rs = avroSchemaMap.get(schemaName);
		else {
			// the required schema is not available in avro schema map, create
			// it
			Type properType = getSpecificTypeFromProtoFile(schemaName);
			if (properType instanceof MessageType)
				rs = getRecordSchema((MessageType) properType, null);
			if (properType instanceof EnumType)
				rs = getEnumSchema((EnumType) properType, null);
			avroSchemaMap.put(rs.getName(), rs);
		}
		return rs;
	}

	private Type getSpecificTypeFromProtoFile(String specific) {
		Type specificType = null;
		for (Type type : protoFile.getTypes()) {
			if (type.getName().equals(specific))
				specificType = type;
		}
		return specificType;
	}

	private Schema getEnumSchema(EnumType enumType, String parentName) {
		String packageName = getPackageName(parentName);
		List<String> enumFields = new ArrayList<String>();
		for (EnumType.Value value : enumType.getValues())
			enumFields.add(value.getName());
		return Schema.createEnum(enumType.getName(), avroDoc, packageName,
				enumFields);
	}

	private Schema getOptionSchema(Schema schema) {
		List<Schema> fields = new ArrayList<Schema>();
		fields.add(schema);
		fields.add(Schema.create(Schema.Type.NULL));
		return Schema.createUnion(fields);
	}

	private String getPackageName(String parentName) {
		String packageName = null;
		if (parentName != null)
			packageName = rootPackageName + "." + parentName;
		else
			packageName = rootPackageName;
		return packageName;
	}
}
