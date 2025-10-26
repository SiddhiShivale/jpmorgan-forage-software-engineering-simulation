package com.jpmc.midascore.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.jpmc.midascore.component.IncentiveApiClient;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;

@Component
public class TransactionConsumer {

	private static final Logger LOGGER = LoggerFactory.getLogger(TransactionConsumer.class);

	private final UserRepository userRepository;
	private final TransactionRepository transactionRepository;
	private final IncentiveApiClient incentiveApiClient;

	public TransactionConsumer(UserRepository userRepository, TransactionRepository transactionRepository,
			IncentiveApiClient incentiveApiClient) {
		this.userRepository = userRepository;
		this.transactionRepository = transactionRepository;
		this.incentiveApiClient = incentiveApiClient;
	}

	@KafkaListener(topics = "${kafka.topic.transactions}", groupId = "${spring.kafka.consumer.group-id}")
	@Transactional
	 public void receiveTransaction(Transaction transaction) {
        LOGGER.info("Received transaction: {}", transaction.toString());

        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null || sender.getBalance() < transaction.getAmount()) {
            LOGGER.warn("Invalid transaction. Discarding.");
            return;
        }

        Incentive incentive = incentiveApiClient.getIncentive(transaction);
        float incentiveAmount = (incentive != null) ? incentive.getAmount() : 0.0f;
        LOGGER.info("Received incentive of {} for transaction.", incentiveAmount);

        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        userRepository.save(sender);
        userRepository.save(recipient);

        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
        transactionRepository.save(transactionRecord);

        LOGGER.info("Successfully processed transaction. New balance for {}: {}, New balance for {}: {}",
                sender.getName(), sender.getBalance(), recipient.getName(), recipient.getBalance());
    }
}