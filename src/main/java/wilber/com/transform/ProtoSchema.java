package wilber.com.transform;

import com.example.tutorial.AddressBookProtos.Person;
import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.DynamicSchema.Builder;
import com.github.os72.protobuf.dynamic.EnumDefinition;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.squareup.protoparser.*;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Stack;

public class ProtoSchema {

	private DynamicSchema dynamicSchema = null;
	private ProtoFile protoFile = null;

	public ProtoSchema() throws DescriptorValidationException, IOException,
			StructureErrorException, UnSupportProtoFormatErrorException {
		ProtoFile protoFile = ProtoSchemaParser.parse(getProtoSourceFile());
		dynamicSchema = getDynamicSchema(protoFile);
	}

	public ProtoSchema(String protoFilePath)
			throws DescriptorValidationException, IOException,
			StructureErrorException, UnSupportProtoFormatErrorException {
		ProtoFile protoFile = ProtoSchemaParser
				.parse(getProtoSourceFile(protoFilePath));
		dynamicSchema = getDynamicSchema(protoFile);
	}

	public DynamicSchema getDynamicSchema() {
		return dynamicSchema;
	}

	public ProtoFile getProtoFile() {
		return protoFile;
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
	
	private String getDefault(Object defaultValue){
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
}
