package wilber.com.transform;

import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;

import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

public class ProtoToAvroTransformer {
	private Map<String, Schema> avroSchemaMap = null;

	public ProtoToAvroTransformer(Map<String, Schema> aSchemaMap) {
		this.avroSchemaMap = aSchemaMap;
	}

	public GenericRecord protoToAvro(DynamicMessage msg) {
		GenericRecord gr = new GenericData.Record(avroSchemaMap.get(msg
				.getDescriptorForType().getName()));
		protoMsgToAvroRecord(msg, gr);
		return gr;
	}

	private void protoMsgToAvroRecord(Message msg, GenericRecord gr) {
		for (Map.Entry<FieldDescriptor, Object> entry : msg.getAllFields()
				.entrySet())
			putToRecord(gr, entry.getKey(), entry.getValue());
	}

	private void putToRecord(GenericRecord gr, FieldDescriptor fd, Object value) {
		String name = fd.getName();
		if (!fd.isRepeated()) {
			gr.put(name, getSingleField(fd, value));
		} else {
			List<?> list = (List<?>) value;
			Schema arraySchema = null;
			if (isPrimitive(fd)) {
				arraySchema = Schema.createArray(Schema
						.create(Utils.ProtoBufPrimitiveTypeMapAvroType.get(fd
								.getType())));
			} else {
				arraySchema = Schema.createArray(avroSchemaMap
						.get(getRepeatedElementTypeName(list.get(0), fd)));
			}
			GenericArray<Object> grA = new GenericData.Array<Object>(
					list.size(), arraySchema);
			for (Object element : list) {
				grA.add(getSingleField(fd, element));
			}
			gr.put(name, grA);
		}
	}

	private GenericRecord nestedMsgToRecord(Message msg, Schema schema) {
		GenericRecord gr = new GenericData.Record(schema);
		protoMsgToAvroRecord(msg, gr);
		return gr;
	}

	private Object getSingleField(FieldDescriptor field, Object value) {
		Object result = null;
		switch (field.getType()) {
		case INT32:
		case SINT32:
		case SFIXED32:
		case INT64:
		case SINT64:
		case SFIXED64:
		case BOOL:
		case FLOAT:
		case DOUBLE:
		case UINT32:
		case FIXED32:
		case UINT64:
		case FIXED64:
		case STRING:
		case BYTES:
			result = value;
			break;
		case ENUM:
			GenericEnumSymbol gEnum = new GenericData.EnumSymbol(
					avroSchemaMap.get(field.getContainingType().getName()),
					((EnumValueDescriptor) value).getName());
			result = gEnum;
			break;

		case MESSAGE:
		case GROUP:
			Message msgValue = (Message) value;
			Schema nestedRecordSchema = avroSchemaMap.get(msgValue
					.getDescriptorForType().getName());
			GenericRecord nestedGr = nestedMsgToRecord(msgValue,
					nestedRecordSchema);
			result = nestedGr;
			break;
		}

		return result;
	}

	private boolean isPrimitive(FieldDescriptor field) {
		boolean result = true;
		switch (field.getType()) {
		case INT32:
		case SINT32:
		case SFIXED32:
		case INT64:
		case SINT64:
		case SFIXED64:
		case BOOL:
		case FLOAT:
		case DOUBLE:
		case UINT32:
		case FIXED32:
		case UINT64:
		case FIXED64:
		case STRING:
		case BYTES:
			result = true;
			break;
		case ENUM:
			result = false;
			break;
		case MESSAGE:
		case GROUP:
			result = false;
			break;
		}
		return result;
	}

	private String getRepeatedElementTypeName(Object value,
			FieldDescriptor field) {
		String name = null;

		switch (field.getType()) {
		case ENUM:
			name = ((EnumValueDescriptor) value).getName();
			break;
		case MESSAGE:
		case GROUP:
			name = ((Message) value).getDescriptorForType().getName();
			break;
		default:
			break;
		}
		return name;
	}
}
