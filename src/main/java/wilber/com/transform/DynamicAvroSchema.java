package wilber.com.transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.swing.DefaultBoundedRangeModel;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.codehaus.jackson.JsonNode;

import com.squareup.protoparser.EnumType;
import com.squareup.protoparser.MessageType;
import com.squareup.protoparser.Option;
import com.squareup.protoparser.ProtoFile;

public class DynamicAvroSchema {

	private Map<String, Schema> avroSchemaMap = new HashMap<String, Schema>();
	private String packageName = null;
	private final String avroDoc = "doc";

	public DynamicAvroSchema(ProtoFile protoFile) {
		packageName = protoFile.getPackageName();
		buildSchemaMap(protoFile);

	}

	private void buildSchemaMap(ProtoFile protoFile) {
		if (!protoFile.getTypes().isEmpty()) {
			for (com.squareup.protoparser.Type type : protoFile.getTypes()) {
				if (!avroSchemaMap.containsKey(type.getName())) {
					if (type instanceof MessageType) {
						MessageType msgType = (MessageType) type;
						avroSchemaMap.put(msgType.getName(),
								getRecordSchema(msgType));
					}
					if (type instanceof EnumType) {
						EnumType enumType = (EnumType) type;
						avroSchemaMap.put(enumType.getName(),
								getEnumSchema(enumType));
					}
				}
			}
		}
	}

	private Schema getRecordSchema(MessageType msgType) {
		Schema rs = Schema.createRecord(msgType.getName(), avroDoc,
				packageName, false);
		List<Field> fields = new ArrayList<Field>();
		for (MessageType.Field field : msgType.getFields()) {
			JsonNode defaultValue = null;
			if (!field.getOptions().isEmpty()) {
				Object defaultSource = Option.findByName(field.getOptions(),
						"default").getValue();
				String defaultString = Utils.getProtoDefault(defaultSource);
			}
			String fieldName = field.getName();
			String fieldType = field.getType();
			Schema fieldSchema = null;
			if (Utils.ProtoBufPrimitiveTypeMap.containsKey(fieldType)) {
				fieldSchema = Schema.create(Utils.ProtoBufPrimitiveTypeMap
						.get(fieldType));
			} else {
				fieldSchema = getProperSchema(fieldType);
			}
			if (field.getLabel().equals("repeated")) {

			} else {

			}
			fields.add(new Field(fieldName, fieldSchema, avroDoc, defaultValue));
		}

		rs.setFields(fields);
		return rs;
	}

	private Schema getProperSchema(String schemaName) {
		Schema rs = null;
		return rs;
	}

	private Schema getEnumSchema(EnumType enumType) {
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

}
