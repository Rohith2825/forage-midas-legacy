package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.Optional;

@Service
public class TransactionListener {

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final RestTemplate restTemplate;

    public TransactionListener(UserRepository userRepository,
                               TransactionRepository transactionRepository,
                               RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group",
            properties = {"spring.json.value.default.type=com.jpmc.midascore.foundation.Transaction"})
    @Transactional
    public void listen(ConsumerRecord<String, Transaction> record) {
        Transaction transaction = record.value();

        Optional<UserRecord> senderOpt = userRepository.findById(transaction.getSenderId());
        Optional<UserRecord> recipientOpt = userRepository.findById(transaction.getRecipientId());

        if (senderOpt.isPresent() && recipientOpt.isPresent()) {
            UserRecord sender = senderOpt.get();
            UserRecord recipient = recipientOpt.get();
            BigDecimal amount = BigDecimal.valueOf(transaction.getAmount());

            if (BigDecimal.valueOf(sender.getBalance()).compareTo(amount) >= 0) {
                // Print sender/recipient balances before processing
                System.out.println("Recipient balance BEFORE processing transaction: " + recipient.getBalance());

                // Deduct the transaction amount from sender and add to recipient
                sender.debit(transaction.getAmount());
                recipient.credit(transaction.getAmount());

                // Print recipient balance after adding the transaction amount
                System.out.println("Recipient balance AFTER transaction amount, BEFORE incentive: " + recipient.getBalance());

                // Call Incentive API to get incentive amount
                BigDecimal incentiveAmount = fetchIncentive(transaction);

                // Print the incentive amount received
                System.out.println("Incentive amount received: " + incentiveAmount.floatValue());

                // Add the incentive to the recipient's balance
                recipient.credit(incentiveAmount.floatValue());

                // Print recipient balance after adding the incentive
                System.out.println("Recipient balance AFTER incentive: " + recipient.getBalance());

                // Save updated balances
                userRepository.save(sender);
                userRepository.save(recipient);

                // Record transaction with incentive
                TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, amount, incentiveAmount);
                transactionRepository.save(transactionRecord);

                System.out.println("Transaction Processed: " + transaction);
            } else {
                System.out.println("Transaction Declined (Insufficient Funds): " + transaction);
            }
        } else {
            System.out.println("Transaction Declined (Invalid Sender/Recipient): " + transaction);
        }
    }

    // 🔹 Call Incentive API
    private BigDecimal fetchIncentive(Transaction transaction) {
        String url = "http://localhost:8080/incentive";
        ResponseEntity<Incentive> response = restTemplate.postForEntity(url, transaction, Incentive.class);
        return response.getBody() != null ? response.getBody().getAmount() : BigDecimal.ZERO;
    }
}