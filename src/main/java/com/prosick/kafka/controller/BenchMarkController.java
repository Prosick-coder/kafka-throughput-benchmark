package com.prosick.kafka.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.prosick.kafka.consumer.TransactionConsumer;
import com.prosick.kafka.producer.KafkaProducer;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
@RequestMapping("/benchmark")
public class BenchMarkController {

	private final KafkaProducer kafkaProducer;
	private final TransactionConsumer transactionConsumer;
	
	@PostMapping("/load")
	public String load(@RequestParam(defaultValue = "2000") int count) {
		
		long start = System.currentTimeMillis();
		for(int i = 0; i < count; i++) {
			kafkaProducer.publishRandomTransaction();
		}
		long timeTaken = System.currentTimeMillis() - start;
		return "Time taken to Produce" + count + " messages is " + timeTaken;
	}
	@GetMapping("/status")
	public String status() {
		return "Processed so far " + transactionConsumer.getProcessedCount();
	}
	@PostMapping("/reset")
	public String reset() {
		transactionConsumer.reset();
	    return "reset";
	}
}
