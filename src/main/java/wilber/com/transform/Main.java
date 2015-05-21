package wilber.com.transform;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.*;
import com.google.protobuf.Descriptors.*;

import java.io.File;
import java.io.InputStream;

public class Main {

	static public void main(String[] args) {
		System.out.println("start protobuf to avro");
		try {
			// Create dynamic schema from proto file in resource
			DynamicProtoSchemaBuilder builder = new DynamicProtoSchemaBuilder();
			DynamicSchema dynamicSchema = builder.buildProtoSchema().build();

			System.out.println("get scheduler builder: "
					+ dynamicSchema.toString());
			// Create dynamic schema
			DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
			schemaBuilder.setName("PersonSchemaDynamic.proto");

			MessageDefinition msgDef = MessageDefinition.newBuilder("Person") // message
																				// Person
					.addField("required", "int32", "id", 1) // required int32 id
															// = 1
					.addField("required", "string", "name", 2) // required
																// string name =
																// 2
					.addField("optional", "string", "email", 3) // optional
																// string email
																// = 3
					.build();

			schemaBuilder.addMessageDefinition(msgDef);
			DynamicSchema schema = schemaBuilder.build();

			// Create dynamic message from schema
			DynamicMessage.Builder msgBuilder = schema
					.newMessageBuilder("Person");
			Descriptor msgDesc = msgBuilder.getDescriptorForType();
			DynamicMessage msg = msgBuilder
					.setField(msgDesc.findFieldByName("id"), 1)
					.setField(msgDesc.findFieldByName("name"), "Alan Turing")
					.setField(msgDesc.findFieldByName("email"), "at@sis.gov.uk")
					.build();

			// System.out.println(schema.parseFrom(msg.toByteArray()).toString());
		} catch (Exception ex) {
			System.out.println("catch error with:" + ex.getMessage());
		}
	}
}
