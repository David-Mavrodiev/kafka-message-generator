package com.sample.kafkasampleapp.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.Random;

@Service
public class AvroSchemaGenerator {
    private final Random random = new Random();

    public static Schema getSchemaFromFile(String filename) {
        try {
            File schemaFile = new File(filename);
            return new Schema.Parser().parse(schemaFile);
        } catch (Exception e) {
            return null;
        }
    }

    public GenericRecord generateRandomData(Schema schema) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);

        for (Schema.Field field : schema.getFields()) {
            builder.set(field, generateRandomDataForField(field));
        }

        return builder.build();
    }

    private Object generateRandomDataForField(Schema.Field field) {
        return switch (field.schema().getType()) {
            case STRING -> "randomString" + random.nextInt(1000);
            case INT -> random.nextInt();
            case LONG -> random.nextLong();
            case BOOLEAN -> random.nextBoolean();
            case DOUBLE -> random.nextDouble();
            case FLOAT -> random.nextFloat();
            // ... handle other types as needed
            default -> null;
        };
    }
}
