package com.sample.kafkasampleapp.controller;

import com.sample.kafkasampleapp.service.KafkaAvroService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@RequestMapping("/generator")
public class GeneratorController {
    @Autowired
    private KafkaAvroService kafkaAvroService;

    @GetMapping("/produce")
    public ResponseEntity<String> produceMessages(
            @RequestParam String topic,
            @RequestParam String schemaPath,
            @RequestParam int messageNumber) throws IOException {

        kafkaAvroService.produce(topic, schemaPath, messageNumber);

        return ResponseEntity.ok("Messages produced successfully!");
    }
}
