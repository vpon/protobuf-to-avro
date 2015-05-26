package wilber.com.transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;

import com.squareup.protoparser.EnumType;
import com.squareup.protoparser.MessageType;
import com.squareup.protoparser.ProtoFile;
import com.squareup.protoparser.Type;

public class DynamicAvroSchema {

	private Map<String, Schema> avroSchemaMap = new HashMap<String, Schema>();
	private String packageName = null;
	private final String enumDoc = "doc";

	public DynamicAvroSchema(ProtoFile protoFile) {
		packageName = protoFile.getPackageName();
		buildSchemaMap(protoFile);

	}

	private void buildSchemaMap(ProtoFile protoFile) {
		if (!protoFile.getTypes().isEmpty()) {
			for (Type type : protoFile.getTypes()) {
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
		Schema rs = null;
		

		return rs;
	}

	private Schema getEnumSchema(EnumType enumType) {
		List<String> enumFields = new ArrayList<String>();
		for (EnumType.Value value : enumType.getValues())
			enumFields.add(value.getName());
		return Schema.createEnum(enumType.getName(), enumDoc, packageName,
				enumFields);
	}

}
