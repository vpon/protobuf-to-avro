package wilber.com.transform;

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.example.tutorial.AddressBookProtos.Person;
import com.google.protobuf.DynamicMessage;

public class Main {

	static public void main(String[] args) {
		System.out.println("start protobuf to avro process test");
		try {
			Person john = Person
					.newBuilder()
					.setId(1234)
					.setName("John Doe")
					.setEmail("jdoe@example.com")
					.addPhone(
							Person.PhoneNumber.newBuilder()
									.setNumber("555-4321")
									.setType(Person.PhoneType.MOBILE))
					.addPhone(
							Person.PhoneNumber.newBuilder()
									.setNumber("123-456798")
									.setType(Person.PhoneType.WORK)).build();

			System.out.println("static message parse:\n"
					+ Person.parseFrom(john.toByteArray()));

			// read .proto and .avsc from source folder
			DynamicProtoSchema dynamicProtoSchema = new DynamicProtoSchema();

			DynamicMessage msg = dynamicProtoSchema.parse(john.toByteArray());
			System.out.println("\ndynamic message parse:\n" + msg);

			DynamicAvroSchema dynamicAvroSchema = new DynamicAvroSchema();
			dynamicAvroSchema.buildSchemaByProtoSchema(dynamicProtoSchema
					.getProtoFile());

			ProtoToAvroTransformer protoToAvroTransformer = new ProtoToAvroTransformer(
					dynamicAvroSchema.getAvroSchemaMap());

			// System.out.println(dynamicAvroSchema.toString());
			// System.out.println(dynamicProtoSchema.toString());
			GenericRecord gr = protoToAvroTransformer.protoToAvro(msg);
			System.out.println("get transform avro record:\n" + gr);

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}
}
