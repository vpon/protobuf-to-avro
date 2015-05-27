package wilber.com.transform;

public class ProtoToAvroTransformer {

	public ProtoToAvroTransformer() {
	}

	// public GenericRecord protoToAvro(DynamicMessage msg)
	// throws InvalidProtocolBufferException,
	// UnSupportProtoFormatErrorException {
	// //GenericRecord gr = new GenericData.Record(avroSchema);
	// //protoMsgToAvroRecord(msg, gr, avroSchema);
	// return gr;
	// }
	//
	// public DynamicSchema getDynamicSchema() {
	// return dynamicSchema;
	// }
	//
	// public ProtoFile getProtoFile() {
	// return protoFile;
	// }
	//
	// private void protoMsgToAvroRecord(Message msg, GenericRecord gr,
	// Schema parentSchema) throws InvalidProtocolBufferException,
	// UnSupportProtoFormatErrorException {
	// for (Map.Entry<FieldDescriptor, Object> entry : msg.getAllFields()
	// .entrySet())
	// putToRecord(gr, entry.getKey(), entry.getValue(), entry.getKey()
	// .getName(), parentSchema);
	// }
	//
	// private void putToRecord(GenericRecord gr, FieldDescriptor fd,
	// Object value, String nestedField, Schema parentSchema)
	// throws InvalidProtocolBufferException,
	// UnSupportProtoFormatErrorException {
	// String name = fd.getName();
	// if (!fd.isRepeated()) {
	// gr.put(name, getSingleField(fd, value, parentSchema));
	// } else {
	// List<?> list = (List<?>) value;
	// Schema nestedSchema = parentSchema.getField(nestedField).schema();
	// GenericArray<Object> grA = new GenericData.Array<Object>(
	// list.size(), nestedSchema);
	// for (Object element : list) {
	// grA.add(getSingleField(fd, element, nestedSchema));
	// }
	// gr.put(name, grA);
	// }
	// }
	//
	// private GenericRecord nestedMsgToRecord(Message msg, String nestedField,
	// Schema parentSchema) throws InvalidProtocolBufferException,
	// UnSupportProtoFormatErrorException {
	// GenericRecord gr = new GenericData.Record(parentSchema.getField(
	// nestedField).schema());
	// protoMsgToAvroRecord(msg, gr, parentSchema);
	// return gr;
	// }
	//
	// private GenericRecord nestedMsgToRecord(Message msg, Schema schema)
	// throws InvalidProtocolBufferException,
	// UnSupportProtoFormatErrorException {
	// GenericRecord gr = new GenericData.Record(schema);
	// protoMsgToAvroRecord(msg, gr, schema);
	// return gr;
	// }
	//
	// private Object getSingleField(FieldDescriptor field, Object value,
	// Schema parentSchema) throws InvalidProtocolBufferException,
	// UnSupportProtoFormatErrorException {
	// Object result = null;
	// switch (field.getType()) {
	// case INT32:
	// case SINT32:
	// case SFIXED32:
	// case INT64:
	// case SINT64:
	// case SFIXED64:
	// case BOOL:
	// case FLOAT:
	// case DOUBLE:
	// case UINT32:
	// case FIXED32:
	// case UINT64:
	// case FIXED64:
	// case STRING:
	// case BYTES:
	// result = value;
	// break;
	// case ENUM:
	// GenericEnumSymbol gEnum = new GenericData.EnumSymbol(parentSchema
	// .getField(field.getName()).schema(),
	// ((EnumValueDescriptor) value).getName());
	// result = gEnum;
	// break;
	//
	// case MESSAGE:
	// case GROUP:
	// Schema eleSchema = parentSchema.getElementType();
	// if (eleSchema != null) {
	// GenericRecord nestedGr = nestedMsgToRecord((Message) value,
	// eleSchema);
	// result = nestedGr;
	// } else {
	// GenericRecord nestedGr = nestedMsgToRecord((Message) value,
	// field.getName(), parentSchema);
	// result = nestedGr;
	// }
	//
	// break;
	// }
	//
	// return result;
	// }
	//
	// private Schema getRecordSchema(MessageType msgType) {
	// Schema rs = null;
	// for (MessageType.Field field : msgType.getFields()) {
	// if (!field.getOptions().isEmpty()) {
	// Object defaultValue = Option.findByName(field.getOptions(),
	// "default").getValue();
	// String defaultString = getDefault(defaultValue);
	// String fieldType = field.getType();
	// if (Utils.ProtoBufPrimitiveTypeMap.containsKey(fieldType)) {
	//
	// } else {
	//
	// }
	// } else {
	//
	// }
	// }
	//
	// for (Type nestedType : msgType.getNestedTypes()) {
	// if (nestedType instanceof MessageType) {
	// MessageType nestedMsgType = (MessageType) nestedType;
	//
	// } else if (nestedType instanceof EnumType) {
	// EnumType nestedEnumType = (EnumType) nestedType;
	//
	// }
	// }
	// return rs;
	// }
	//
	// private Schema getEnumSchema(EnumType enumType) {
	// List<String> enumFields = new ArrayList<String>();
	// for (EnumType.Value value : enumType.getValues())
	// enumFields.add(value.getName());
	//
	// return Schema.createEnum(enumType.getName(), "",
	// protoFile.getPackageName(), enumFields);
	// }
}
