package wilber.com.transform;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.EnumDefinition;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.squareup.protoparser.*;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;

public class ProtoToAvro {

	private DynamicSchema dynamicSchema = null;
	private ProtoFile protoFile = null;
	private List<DynamicMessage.Builder> dMBuilder = new ArrayList<DynamicMessage.Builder>();
	private Schema avroSchema = null;
	private DynamicMessage.Builder msgBuilder = null;

	public ProtoToAvro() throws DescriptorValidationException, IOException,
			StructureErrorException, UnSupportProtoFormatErrorException {
		ProtoFile protoFile = ProtoSchemaParser.parse(getProtoSourceFile());
		dynamicSchema = getDynamicSchema(protoFile);
		buildDmBuilder(protoFile.getTypes());
		avroSchema = new Schema.Parser().parse(getAvscSourceFile());
	}

	public ProtoToAvro(String protoFilePath, String avscFilePath)
			throws DescriptorValidationException, IOException,
			StructureErrorException, UnSupportProtoFormatErrorException {
		ProtoFile protoFile = ProtoSchemaParser
				.parse(getProtoSourceFile(protoFilePath));
		dynamicSchema = getDynamicSchema(protoFile);
		buildDmBuilder(protoFile.getTypes());
		avroSchema = new Schema.Parser().parse(getAvscSourceFile(avscFilePath));
	}

	public GenericRecord protoToAvro(DynamicMessage msg)
			throws InvalidProtocolBufferException,
			UnSupportProtoFormatErrorException {
		GenericRecord gr = new GenericData.Record(avroSchema);
		protoMsgToAvroRecord(msg, gr, avroSchema);
		return gr;
	}

	private void protoMsgToAvroRecord(Message msg, GenericRecord gr,
			Schema parentSchema) throws InvalidProtocolBufferException,
			UnSupportProtoFormatErrorException {
		for (Map.Entry<FieldDescriptor, Object> entry : msg.getAllFields()
				.entrySet()) {
			String fieldName = entry.getKey().getName();
			putToRecord(gr, entry.getKey(), entry.getValue(), fieldName,
					parentSchema);
		}

	}

	private void putToRecord(GenericRecord gr, FieldDescriptor fd,
			Object value, String nestedField, Schema parentSchema)
			throws InvalidProtocolBufferException,
			UnSupportProtoFormatErrorException {
		String name = fd.getName();
		if (!fd.isRepeated()) {
			gr.put(name, getSingleField(fd, value, parentSchema));
		} else {
			List<?> list = (List<?>) value;
			Schema nestedSchema = parentSchema.getField(nestedField).schema();
			GenericArray<Object> grA = new GenericData.Array<Object>(
					list.size(), nestedSchema);
			for (Object element : list) {
				grA.add(getSingleField(fd, element, nestedSchema));
			}
			gr.put(name, grA);
		}
	}

	private GenericRecord nestedMsgToRecord(Message msg, String nestedField,
			Schema parentSchema) throws InvalidProtocolBufferException,
			UnSupportProtoFormatErrorException {
		GenericRecord gr = new GenericData.Record(parentSchema.getField(
				nestedField).schema());
		protoMsgToAvroRecord(msg, gr, parentSchema);
		return gr;
	}
	
	private GenericRecord nestedMsgToRecord(Message msg,
			Schema schema) throws InvalidProtocolBufferException,
			UnSupportProtoFormatErrorException {
		GenericRecord gr = new GenericData.Record(schema);
		protoMsgToAvroRecord(msg, gr, schema);
		return gr;
	}

	private Object getSingleField(FieldDescriptor field, Object value,
			Schema parentSchema) throws InvalidProtocolBufferException,
			UnSupportProtoFormatErrorException {
		Object result = null;
		switch (field.getType()) {
		case INT32:
		case SINT32:
		case SFIXED32:
			result = (Integer) value;
			break;

		case INT64:
		case SINT64:
		case SFIXED64:
			result = (Long) value;
			break;

		case BOOL:
			result = (Boolean) value;
			break;

		case FLOAT:
			result = (Float) value;
			break;

		case DOUBLE:
			result = (Double) value;
			break;

		case UINT32:
		case FIXED32:
			result = (Integer) value;
			break;

		case UINT64:
		case FIXED64:
			result = (Long) value;
			break;
		case STRING:
			result = (String) value;
			break;
		case ENUM:
			GenericEnumSymbol gEnum = new GenericData.EnumSymbol(parentSchema
					.getField(field.getName()).schema(),
					((EnumValueDescriptor) value).getName());
			result = gEnum;
			break;

		case MESSAGE:
		case GROUP:
			Schema eleSchema = parentSchema.getElementType();
			if (eleSchema != null) {
				GenericRecord nestedGr = nestedMsgToRecord((Message) value,
						eleSchema);
				result = nestedGr;
			} else {
				GenericRecord nestedGr = nestedMsgToRecord((Message) value,
						field.getName(), parentSchema);
				result = nestedGr;
			}

			break;
		case BYTES:
			result = (byte[]) value;
			break;
		}

		return result;
	}

	public DynamicMessage parse(byte[] msg)
			throws InvalidProtocolBufferException,
			UnSupportProtoFormatErrorException {
		DynamicMessage.Builder msgBuilder = getProperDmBuilder(msg);
		if (msgBuilder == null)
			throw new UnSupportProtoFormatErrorException(
					"cannot find proper schema for this protocol buffer message,"
							+ " maybe provide worng schema file");

		return msgBuilder.mergeFrom(msg).build();
	}

	private DynamicMessage.Builder getProperDmBuilder(byte[] msg) {
		DynamicMessage.Builder properBuilder = null;
		for (DynamicMessage.Builder untestedBuilder : dMBuilder) {
			properBuilder = testDmBuilder(untestedBuilder, msg);
			if (properBuilder != null)
				break;
		}
		msgBuilder = properBuilder;
		return properBuilder;
	}

	private DynamicMessage.Builder testDmBuilder(
			DynamicMessage.Builder testBuilder, byte[] msg) {
		DynamicMessage.Builder properBuilder = null;
		try {
			testBuilder.mergeFrom(msg);
			properBuilder = testBuilder;
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace(System.out);
		}
		return properBuilder;
	}

	public DynamicSchema getDynamicSchema() {
		return dynamicSchema;
	}

	public ProtoFile getProtoFile() {
		return protoFile;
	}

	private void buildDmBuilder(List<Type> types) {
		for (Type type : types) {
			if (type instanceof MessageType) {
				dMBuilder.add(dynamicSchema.newMessageBuilder(type.getName()));
			}
		}
	}

	private DynamicSchema getDynamicSchema(ProtoFile protoFile)
			throws IOException, StructureErrorException,
			UnSupportProtoFormatErrorException, DescriptorValidationException {
		DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();

		schemaBuilder.setName(protoFile.getFileName());
		schemaBuilder.setPackage(protoFile.getPackageName());

		if (!protoFile.getTypes().isEmpty()) {
			for (Type type : protoFile.getTypes()) {
				if (type instanceof MessageType) {
					MessageType msgType = (MessageType) type;
					schemaBuilder
							.addMessageDefinition(getMsgDefinition(msgType));
				} else if (type instanceof EnumType) {
					EnumType enumType = (EnumType) type;
					schemaBuilder
							.addEnumDefinition(getEnumDefinition(enumType));
				}
			}
		}

		return schemaBuilder.build();
	}

	private MessageDefinition getMsgDefinition(MessageType msgType) {
		MessageDefinition.Builder msgBuilder = MessageDefinition
				.newBuilder(msgType.getName());
		for (MessageType.Field field : msgType.getFields()) {
			if (!field.getOptions().isEmpty()) {
				Object defaultValue = Option.findByName(field.getOptions(),
						"default").getValue();
				String defaultString = getDefault(defaultValue);
				msgBuilder.addField(field.getLabel().toString().toLowerCase()
						.toLowerCase(Locale.US), field.getType(),
						field.getName(), field.getTag(), defaultString);
			} else {
				msgBuilder.addField(field.getLabel().toString().toLowerCase()
						.toLowerCase(Locale.US), field.getType(),
						field.getName(), field.getTag());
			}
		}

		for (Type nestedType : msgType.getNestedTypes()) {
			if (nestedType instanceof MessageType) {
				MessageType nestedMsgType = (MessageType) nestedType;
				msgBuilder
						.addMessageDefinition(getMsgDefinition(nestedMsgType));

			} else if (nestedType instanceof EnumType) {
				EnumType nestedEnumType = (EnumType) nestedType;
				msgBuilder.addEnumDefinition(getEnumDefinition(nestedEnumType));
			}
		}
		return msgBuilder.build();
	}

	private String getDefault(Object defaultValue) {
		String defaultString = null;

		if (defaultValue instanceof EnumType.Value) {
			defaultString = ((EnumType.Value) defaultValue).getName();
		} else {
			defaultString = (String) defaultValue;
		}
		return defaultString;
	}

	private EnumDefinition getEnumDefinition(EnumType enumType) {
		EnumDefinition.Builder enumBuilder = EnumDefinition.newBuilder(enumType
				.getName());

		for (EnumType.Value value : enumType.getValues()) {
			enumBuilder.addValue(value.getName(), value.getTag());
		}

		return enumBuilder.build();
	}

	private File getAvscSourceFile(String path) {
		File avsc = null;
		try {
			avsc = new File(path);
		} catch (Exception ex) {
			System.out.println("unable to access avsc file with exception:"
					+ ex.getMessage());
		}
		return avsc;
	}

	private File getAvscSourceFile() {
		File avsc = null;
		ClassLoader classLoader = getClass().getClassLoader();
		try {
			avsc = new File(classLoader.getResource("addressbook.avsc")
					.getFile());
		} catch (Exception ex) {
			System.out.println("unable to access avsc file with exception:"
					+ ex.getMessage());
		}
		return avsc;
	}

	private File getProtoSourceFile() {
		File protof = null;
		ClassLoader classLoader = getClass().getClassLoader();
		try {
			protof = new File(classLoader.getResource("addressbook.proto")
					.getFile());
		} catch (Exception ex) {
			System.out.println("unable to access proto file with exception:"
					+ ex.getMessage());
		}
		return protof;
	}

	private File getProtoSourceFile(String path) {
		File protof = null;
		try {
			protof = new File(path);
		} catch (Exception ex) {
			System.out.println("unable to access proto file with exception:"
					+ ex.getMessage());
		}
		return protof;
	}
}
