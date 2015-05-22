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

		if (!protoFile.getTypes().isEmpty()) {
			for (Type type : protoFile.getTypes()) {
				if (type instanceof MessageType) {
					System.out.println("get meeage type :\n" + type.getName());
					for (Type ntype : type.getNestedTypes())
						System.out.println("nested type:" + ntype.getClass());

				}
			}
		}
		;

		return schemaBuilder.build();
	}

	public File getProtoSourceFile(String path) {
		File protof = null;
		try {
			protof = new File(path);
		} catch (Exception ex) {
			System.out.println("unable to access proto file with exception:"
					+ ex.getMessage());
		}
		return protof;
	}

	public File getProtoSourceFile() {
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
