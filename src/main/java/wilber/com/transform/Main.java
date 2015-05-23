package wilber.com.transform;

import com.example.tutorial.AddressBookProtos.Person;

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
									.setType(Person.PhoneType.HOME)).build();

			System.out
					.println("parse person message by static person schema:\n"
							+ Person.parseFrom(john.toByteArray()).toString());

			ProtoSchema protoSchema = new ProtoSchema();
			System.out.println("dynamic message parse:\n" + protoSchema.parse(john.toByteArray()));
		} catch (Exception ex) {
			System.out.println("catch error with:\n" + ex.toString());
			ex.printStackTrace(System.out);
		}
	}
}
