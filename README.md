protobuf-to-avro
================
###Dynamically parse protocol buffer message by .proto schema and convert to correspond avro record 
---
#####This project depends on https://github.com/os72/protobuf-dynamic and https://github.com/square/protoparser  
#####Thanks for their contribution  
    

---  

#### Usage
Before you use it, you need to provide .proto schema file(put in source folder under maven project or provide file path).    
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
					.setType(Person.PhoneType.MOBILE)
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
```
   
If any question , feel free to contact me:cecol3500123@gmail.com