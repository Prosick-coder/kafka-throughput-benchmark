package com.prosick.kafka.model;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {
	 	private String transactionId;
	    private String senderAccount;
	    private String receiverAccount;
	    private BigDecimal amount;
	    private String currency;
	    private long producedAtEpochMillis; // for measuring latency later
}
