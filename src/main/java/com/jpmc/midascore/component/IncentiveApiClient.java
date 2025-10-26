package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveApiClient {

    private final RestTemplate restTemplate;
    private static final String INCENTIVE_API_URL = "http://localhost:8080/incentive";

    public IncentiveApiClient(RestTemplateBuilder builder) {
        this.restTemplate = builder.build();
    }

    public Incentive getIncentive(Transaction transaction) {
        return restTemplate.postForObject(INCENTIVE_API_URL, transaction, Incentive.class);
    }
}