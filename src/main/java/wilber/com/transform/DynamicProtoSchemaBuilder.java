package wilber.com.transform;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.DynamicSchema.Builder;
import com.github.os72.protobuf.dynamic.EnumDefinition;
import com.github.os72.protobuf.dynamic.MessageDefinition;

import static com.github.protobufel.MessageAdapter.*;

import com.github.protobufel.DynamicMessage;
import com.github.protobufel.grammar.ProtoFiles;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

public class DynamicProtoSchemaBuilder {

    private final String CurlyBraceHead = "{";
    private final String CurlyBraceEnd = "}";
    private Stack<String> blockStringStack = new Stack<String>();
    private Stack<MessageDefinition.Builder> messageBuilderStack = new Stack<MessageDefinition.Builder>();
    private Stack<EnumDefinition.Builder> enumBuilderStack = new Stack<EnumDefinition.Builder>();
    private final String MESSAGE = "message";
    private final String ENUM = "enum";
    private String schemaName = null;

    public void buildDynamicSchemaByDescript() {
        System.out.println("get in FileDescriptor1");
        File proto = getProtoFile();
        Map<String, FileDescriptor> fileDescriptors =
                ProtoFiles.newBuilder().addFiles(new File(proto.getParent()), proto.getPath()).build();
        System.out.println("get in FileDescriptor2: " + proto.getName()+" "+fileDescriptors.);
        FileDescriptor fileDescriptor = fileDescriptors.get(proto.getName());
        System.out.println("get FileDescriptor:\n"+fileDescriptor.toString());
    }

    public DynamicSchema.Builder buildDynamicSchema() throws IOException,
            StructureErrorException, UnSupportProtoFormatErrorException {
        DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
        File protoFile = getProtoFile();
        schemaBuilder.setName(protoFile.getName());
        BufferedReader br = new BufferedReader(new FileReader(protoFile));

        String line = null;
        while ((line = br.readLine()) != null) {
            if (!StringUtils.isBlank(line)) {
                // filter empty line
                if (!blockStringStack.empty()) {
                    // check is nested block status or not
                    if (!line.contains(CurlyBraceEnd)
                            && !line.contains(CurlyBraceHead)) {
                        // this line is field
                        String previousBlock = blockStringStack.pop();
                        checkTypeAndAddFiledToBuilder(line, previousBlock);
                        blockStringStack.push(previousBlock);

                    } else if (line.contains(CurlyBraceEnd)) {
                        // this line is end of block
                        String previousBlock = blockStringStack.pop();
                        concludeBlockStack(previousBlock, schemaBuilder);
                    } else if (line.contains(CurlyBraceHead)) {
                        // this line is another nested block start
                        checkTypeAndCreateBuilderToStack(line);
                        blockStringStack.push(line);
                    }
                } else if (line.contains(CurlyBraceHead)) {
                    // no block now, check is block start or not
                    blockStringStack.push(line);
                    checkTypeAndCreateBuilderToStack(line);
                }
            }
        }
        br.close();
        return schemaBuilder;
    }

    private void concludeBlockStack(String previousBlock,
                                    DynamicSchema.Builder schemaBuilder)
            throws StructureErrorException, UnSupportProtoFormatErrorException {
        if (blockStringStack.isEmpty()) {
            // last curly brace, which means block is end, then add to schema
            attributeToSchema(previousBlock, schemaBuilder);
        } else {
            String messageBlock = blockStringStack.pop();
            if (messageBlock.toLowerCase().contains(MESSAGE)) {
                attributeToMessage(previousBlock);
                blockStringStack.push(messageBlock);
            } else {
                throw new UnSupportProtoFormatErrorException(
                        "this string contain syntax error on curly brace: "
                                + previousBlock);
            }
        }
    }

    private void attributeToMessage(String previousBlock)
            throws StructureErrorException {
        if (previousBlock.toLowerCase().contains(MESSAGE)) {
            MessageDefinition.Builder msgBuilderCallee = messageBuilderStack
                    .pop();
            MessageDefinition.Builder msgBuilderCaller = messageBuilderStack
                    .pop();
            msgBuilderCaller.addMessageDefinition(msgBuilderCallee.build());
            messageBuilderStack.push(msgBuilderCaller);
        } else if (previousBlock.toLowerCase().contains(ENUM)) {
            EnumDefinition.Builder enumBuilderCallee = enumBuilderStack.pop();
            MessageDefinition.Builder msgBuilderCaller = messageBuilderStack
                    .pop();
            msgBuilderCaller.addEnumDefinition(enumBuilderCallee.build());
            messageBuilderStack.push(msgBuilderCaller);
        } else
            throw new StructureErrorException(
                    "error structure when try to constructure message");
    }

    private void attributeToSchema(String protoString,
                                   DynamicSchema.Builder schemaBuilder) throws StructureErrorException {
        if (protoString.toLowerCase().contains(MESSAGE)) {
            if (messageBuilderStack.isEmpty()) {
                throw new StructureErrorException(
                        "error structure when try to constructure message");
            } else {
                MessageDefinition.Builder messageBuilder = messageBuilderStack
                        .pop();
                schemaBuilder.addMessageDefinition(messageBuilder.build());
            }
        } else if (protoString.toLowerCase().contains(ENUM)) {
            if (enumBuilderStack.isEmpty()) {
                throw new StructureErrorException(
                        "error structure when try to constructure enmu");
            } else {
                EnumDefinition.Builder enmuBuilder = enumBuilderStack.pop();
                schemaBuilder.addEnumDefinition(enmuBuilder.build());
            }
        }
    }

    private void checkTypeAndCreateBuilderToStack(String protoString) {
        if (protoString.toLowerCase().contains(MESSAGE)) {
            String messageName = protoString.trim().replaceAll(" +", " ")
                    .split(" ")[1].trim();
            // System.out.println("create message name:" + messageName);
            MessageDefinition.Builder msgBuilder = MessageDefinition
                    .newBuilder(messageName);
            messageBuilderStack.push(msgBuilder);
        } else if (protoString.toLowerCase().contains(ENUM)) {
            String enumName = protoString.trim().replaceAll(" +", " ")
                    .split(" ")[1].trim();
            // System.out.println("create message name:" + enumName);
            EnumDefinition.Builder enumBuilder = EnumDefinition
                    .newBuilder(enumName);
            enumBuilderStack.push(enumBuilder);
        }
    }

    private void checkTypeAndAddFiledToBuilder(String protoString,
                                               String previousBlock) {
        if (previousBlock.toLowerCase().contains(MESSAGE)) {
            MessageDefinition.Builder msgBuilder = messageBuilderStack.pop();

            String[] messages = protoString.trim().replaceAll(" +", " ")
                    .split("=");
            String fieldSignature = messages[0].trim();
            String fieldNum = messages[1].trim();
            if (fieldNum.contains(";"))// no default field
            {
                String[] fields = fieldSignature.split(" ");
                msgBuilder.addField(fields[0].trim(), fields[1].trim(),
                        fields[2].trim(),
                        Character.getNumericValue(fieldNum.charAt(0)));
                // System.out.println("add to message:" + fields[0].trim() + " "
                // + fields[1].trim() + " " + fields[2].trim() + " "
                // + Character.getNumericValue(fieldNum.charAt(0)));
            } else {
                String[] fields = fieldSignature.split(" ");
                String defaultValue = messages[2].trim().split(" ")[0];
                if (defaultValue.contains("]"))
                    defaultValue = defaultValue.substring(0,
                            defaultValue.length() - 2);
                msgBuilder.addField(fields[0].trim(), fields[1].trim(),
                        fields[2].trim(),
                        Character.getNumericValue(fieldNum.charAt(0)),
                        defaultValue);
                // System.out.println("add to message:" + fields[0].trim() + " "
                // + fields[1].trim() + " " + fields[2].trim() + " "
                // + Character.getNumericValue(fieldNum.charAt(0)) + " "
                // + defaultValue);
            }

            messageBuilderStack.push(msgBuilder);
        } else if (previousBlock.toLowerCase().contains(ENUM)) {
            EnumDefinition.Builder enumBuilder = enumBuilderStack.pop();
            String[] messages = protoString.trim().replaceAll(" +", " ")
                    .split("=");
            String fieldSignature = messages[0].trim();
            String fieldNum = messages[1].trim();
            enumBuilder.addValue(fieldSignature,
                    Character.getNumericValue(fieldNum.charAt(0)));
            enumBuilderStack.push(enumBuilder);
            // System.out.println("add to enum:" + fieldSignature + " "
            // + Character.getNumericValue(fieldNum.charAt(0)));
        }
    }

    public File getProtoFile(String path) {
        File protof = null;
        try {
            protof = new File(path);
        } catch (Exception ex) {
            System.out.println("unable to access proto file with exception:"
                    + ex.getMessage());
        }
        return protof;
    }

    public File getProtoFile() {
        File protof = null;
        ClassLoader classLoader = getClass().getClassLoader();
        try {
            protof = new File(classLoader.getResource("addressbook.proto")
                    .getFile());
        } catch (Exception ex) {
            System.out.println("unable to access proto file with exception:"
                    + ex.getMessage());
        }
        return protof;
    }
}
