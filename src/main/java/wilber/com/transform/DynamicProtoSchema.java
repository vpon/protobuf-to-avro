package wilber.com.transform;

import java.io.File;
import java.io.IOException;
import java.util.Locale;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.EnumDefinition;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.squareup.protoparser.EnumType;
import com.squareup.protoparser.MessageType;
import com.squareup.protoparser.Option;
import com.squareup.protoparser.ProtoFile;
import com.squareup.protoparser.ProtoSchemaParser;
import com.squareup.protoparser.Type;

public class DynamicProtoSchema {
	private DynamicSchema dynamicSchema = null;
	private ProtoFile protoFile = null;

	public DynamicProtoSchema() throws DescriptorValidationException,
			IOException, StructureException, UnSupportProtoFormatErrorException {
		protoFile = ProtoSchemaParser.parse(getProtoSourceFile());
		dynamicSchema = getDynamicSchema(protoFile);
	}

	public DynamicProtoSchema(String protoFilePath)
			throws DescriptorValidationException, IOException,
			StructureException, UnSupportProtoFormatErrorException {
		protoFile = ProtoSchemaParser.parse(getProtoSourceFile(protoFilePath));
		dynamicSchema = getDynamicSchema(protoFile);
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
		for (Type type : protoFile.getTypes())
			if (type instanceof MessageType) {
				properBuilder = testDmBuilder(type.getName(), msg);
				if (properBuilder != null)
					break;
			}
		return properBuilder;
	}

	private DynamicMessage.Builder testDmBuilder(String testName, byte[] testMsg) {
		DynamicMessage.Builder properBuilder = null;
		try {
			DynamicMessage.Builder testBuilder = dynamicSchema
					.newMessageBuilder(testName);
			testBuilder.mergeFrom(testMsg);
			properBuilder = testBuilder.clear();
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

	private DynamicSchema getDynamicSchema(ProtoFile protoFile)
			throws IOException, StructureException,
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

	private File getProtoSourceFile() {
		File protof = null;
		ClassLoader classLoader = getClass().getClassLoader();
		try {
			File[] sourceFolderFiles = new File(classLoader.getResource("")
					.getFile()).listFiles();
			for (File file : sourceFolderFiles) {
				if (file.getName().contains(".proto")) {
					protof = file;
					break;
				}
			}
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
