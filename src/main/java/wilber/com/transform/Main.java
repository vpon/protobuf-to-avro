package wilber.com.transform;

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
									.setType(Person.PhoneType.MOBILE)).build();

			System.out.println("static message parse:\n"
					+ Person.parseFrom(john.toByteArray()));

			// read .proto and .avsc from source folder
			DynamicProtoSchema dynamicProtoSchema = new DynamicProtoSchema();

			DynamicMessage msg = dynamicProtoSchema.parse(john.toByteArray());
			System.out.println("dynamic message parse:\n" + msg);

			//GenericRecord gr = protoSchema.protoToAvro(msg);
			//System.out.println("transform proto to avro:\n" + gr.toString());

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}
}
