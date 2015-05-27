package vpon.com.transform;

import java.io.File;
import java.io.IOException;
import java.util.Locale;

import org.javatuples.Pair;

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
			IOException, StructureException, UnSupportProtoFormatException {
		protoFile = ProtoSchemaParser.parse(getProtoSourceFile());
		dynamicSchema = getDynamicSchema(protoFile);
	}

	public DynamicProtoSchema(String protoFilePath)
			throws DescriptorValidationException, IOException,
			StructureException, UnSupportProtoFormatException {
		protoFile = ProtoSchemaParser.parse(getProtoSourceFile(protoFilePath));
		dynamicSchema = getDynamicSchema(protoFile);
	}

	public DynamicMessage parse(byte[] msg)
			throws InvalidProtocolBufferException,
			UnSupportProtoFormatException {
		DynamicMessage dMsg = getProperDmBuilder(msg);
		if (dMsg == null)
			throw new UnSupportProtoFormatException(
					"cannot find proper schema for this protocol buffer message,"
							+ " maybe provide worng schema file");

		return dMsg;
	}

	private DynamicMessage getProperDmBuilder(byte[] msg) {
		DynamicMessage properBuilder = null;
		String name = null;
		for (Type type : protoFile.getTypes())
			if (type instanceof MessageType) {
				properBuilder = testDmBuilder(type.getName(), msg);
				if (properBuilder != null) {
					name = type.getName();
					break;
				}
			}
		return properBuilder;
	}

	private DynamicMessage testDmBuilder(String testName, byte[] testMsg) {
		DynamicMessage properBuilder = null;
		try {
			DynamicMessage.Builder testBuilder = dynamicSchema
					.newMessageBuilder(testName);
			testBuilder.mergeFrom(testMsg);
			properBuilder = testBuilder.build();
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
			UnSupportProtoFormatException, DescriptorValidationException {
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

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Protocol buffer Schema:\n");
		sb.append(dynamicSchema.toString());
		return sb.toString();
	}
}
