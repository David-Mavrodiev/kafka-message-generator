package com.sample.kafkasampleapp.service;

import com.sample.kafkasampleapp.util.AvroSchemaGenerator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;

@Service
public class KafkaAvroService {
    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @Autowired
    private AvroSchemaGenerator generator;

    public void produce(String topic, String schemaPath, int count) throws IOException {
        Schema schema =
                AvroSchemaGenerator.getSchemaFromFile(Path.of(schemaPath).toString());

        for (int i = 0; i < count; i++) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

            GenericRecord record = generator.generateRandomData(schema);
            datumWriter.write(record, encoder);
            encoder.flush();
            out.close();

            byte[] serializedBytes = out.toByteArray();
            kafkaTemplate.send(topic, record.toString(), serializedBytes);
        }
    }

    @KafkaListener(topics = "my-topic", groupId = "group-id")
    public void consume(String key, byte[] bytes) throws IOException {
        Schema schema =
                AvroSchemaGenerator.getSchemaFromFile(Path.of(schemaPath).toString());
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        GenericRecord user = datumReader.read(null, decoder);

        System.out.println("Consumed user with name: " + user.toString());
    }
}

