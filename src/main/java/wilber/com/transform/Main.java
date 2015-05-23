package wilber.com.transform;

import com.example.tutorial.AddressBookProtos.Person;
import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.EnumDefinition;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.DynamicMessage;

public class Main {

	static public void main(String[] args) {
		System.out.println("start protobuf to avro");
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
			DynamicSchema dynamicSchema = protoSchema.getDynamicSchema();
			System.out.println("get dynamic schema from .proto file:\n"
					+ dynamicSchema.toString());
			DynamicMessage.Builder msgBuilder = dynamicSchema
					.newMessageBuilder("Person");
			System.out.println("merge message");
			DynamicMessage msg = msgBuilder.mergeFrom(john.toByteArray())
					.build();

			System.out.println("dynamic message parse:\n" + msg);
		} catch (Exception ex) {
			System.out.println("catch error with:\n" + ex.toString());
			ex.printStackTrace(System.out);
		}
	}
}
