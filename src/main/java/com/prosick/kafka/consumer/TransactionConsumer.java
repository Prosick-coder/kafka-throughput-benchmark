package com.prosick.kafka.consumer;

import java.util.concurrent.atomic.LongAdder;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.prosick.kafka.model.Transaction;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class TransactionConsumer {

	private final LongAdder processedCount = new LongAdder();
	
	@KafkaListener(topics = "banking-transaction", groupId = "transaction-processor-group")
	public void consume(ConsumerRecord<String, Transaction> record) {
		
		Transaction tx = record.value();
		long latency = System.currentTimeMillis() - tx.getProducedAtEpochMillis();
		
		// Simulated processing work — without this delay, 1 thread vs many
        // would look identical because there's nothing to actually parallelize.
		try {
			Thread.sleep(2);
		}catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		
		processedCount.increment();
		
		if(processedCount.longValue() % 500 == 0) {
			log.info("Thread [{}] processed {} so far, partition [{}], latency [{}]"
					,Thread.currentThread().getName(), processedCount.longValue(), record.partition()
					,latency);
		}
	}
	public long getProcessedCount() {
		return processedCount.longValue();
	}
	public void reset() {
	    processedCount.reset();
	}
}
