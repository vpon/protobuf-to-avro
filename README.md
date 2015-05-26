protobuf-to-avro
================
###Dynamically parse protocol buffer message by .proto schema and convert to correspond avro record 
---
#####This project depends on https://github.com/os72/protobuf-dynamic and https://github.com/square/protoparser  
#####Thanks for their contribution  
    

---  

#### Usage
Before you use it, you need to provide .proto and .avsc schema file(put in source folder under maven project or provide file path).  
And you need to make sure these two schema structure are identical, otherwise it will throw structure error exception.  
Example:
```java
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

	System.out.println("static message parse:\n" + Person.parseFrom(john.toByteArray()));

	// read .proto and .avsc from source folder, and use them them to build dynamic schema for converting
	ProtoToAvro protoSchema = new ProtoToAvro();
		
	DynamicMessage msg = protoSchema.parse(john.toByteArray());
	System.out.println("dynamic message parse:\n" + msg);

	GenericRecord gr = protoSchema.protoToAvro(msg);
	System.out.println("transform proto to avro:\n" + gr.toString());

} catch (Exception ex) {
	ex.printStackTrace(System.out);
}
```
   
If any question , feel free to contact me:cecol3500123@gmail.com