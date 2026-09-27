package com.prosick.kafka.producer;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.prosick.kafka.model.Transaction;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class KafkaProducer {

	private final KafkaTemplate<String, Transaction> kafkaTemplate;
	
	public void publishRandomTransaction() {
		Transaction tx = new Transaction(
                UUID.randomUUID().toString(),
                "ACC" + ThreadLocalRandom.current().nextInt(10000, 99999),
                "ACC" + ThreadLocalRandom.current().nextInt(10000, 99999),
                BigDecimal.valueOf(ThreadLocalRandom.current().nextDouble(10, 50000)),
                "USD",
                System.currentTimeMillis()
        );
		
		// Keying by transactionId spreads messages evenly across the 6 partitions —
        // this is what makes multi-threaded consumption actually parallel later.
		
		kafkaTemplate.send("banking-transaction", tx.getTransactionId(), tx);
	}
	
}
