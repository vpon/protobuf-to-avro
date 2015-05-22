package wilber.com.transform;

import com.example.tutorial.AddressBookProtos.Person;
import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.EnumDefinition;
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
			DynamicProtoSchemaBuilder dPBuilder = new DynamicProtoSchemaBuilder();
			DynamicSchema dynamicSchema = dPBuilder.buildDynamicSchema().build();

			System.out.println("get scheduler builder:\n"
					+ dynamicSchema.toString());

			Person john = Person
					.newBuilder()
					.setId(1234)
					.setName("John Doe")
					.setEmail("jdoe@example.com")
					.addPhone(
							Person.PhoneNumber.newBuilder()
									.setNumber("555-4321")
									.setType(Person.PhoneType.HOME)).build();

			System.out
					.println("Person message from static java code:\n" + john.toString() + "\n");

			System.out
					.println("parse person message by static person schema:\n"
							+ Person.parseFrom(john.toByteArray()).toString());

			// System.out.println("parse person message by dynamic schema:"
			// + dynamicSchema.parseFrom(john.toByteArray()).toString());

			// Create dynamic schema
			DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
			schemaBuilder = schemaBuilder.setName("AddressBookProtos")
					.setPackage("com.example.tutorial");

			MessageDefinition msgPersonDef = MessageDefinition
					.newBuilder("Person")
					.addField("required", "string", "name", 1)
					.addField("required", "int32", "id", 2)
					.addField("optional", "string", "email", 3)
					.addEnumDefinition(
							EnumDefinition.newBuilder("PhoneType")
									.addValue("MOBILE", 0).addValue("HOME", 1)
									.addValue("WORK", 2).build())
					.addMessageDefinition(
							MessageDefinition
									.newBuilder("PhoneNumber")
									.addField("required", "string", "number", 1)
									.addField("optional", "PhoneType", "type",
											2, "HOME").build())
					.addField("repeated", "PhoneNumber", "phone", 4).build();

			MessageDefinition msgAddressDef = MessageDefinition
					.newBuilder("AddressBook")
					.addField("repeated", "Person", "person", 1).build();

			schemaBuilder.addMessageDefinition(msgPersonDef);
			schemaBuilder.addMessageDefinition(msgAddressDef);
			DynamicSchema personSchema = schemaBuilder.build();
			System.out.println("build schema from java code:\n"
					+ personSchema.toString());

			System.out.println("2:\n"
					+ personSchema.parseFrom(john.toByteArray()).toString());
		} catch (Exception ex) {
			System.out.println("catch error with:" + ex.getMessage());
		}
	}
}
