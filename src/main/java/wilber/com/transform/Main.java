package wilber.com.transform;

import java.util.Arrays;

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
									.setType(Person.PhoneType.MOBILE)).build();

			System.out.println("static message parse:\n"
					+ Person.parseFrom(john.toByteArray()));

			ProtoToAvro protoSchema = new ProtoToAvro();
			// read .proto and .avsc from source folder

			DynamicMessage msg = protoSchema.parse(john.toByteArray());
			System.out.println("dynamic message parse:\n" + msg);

			GenericRecord gr = protoSchema.protoToAvro(msg);
			System.out.println("transform proto to avro:\n" + gr.toString());

		} catch (Exception ex) {
			System.out.println("catch error with:\n" + ex.toString());
			ex.printStackTrace(System.out);
		}
	}
}
