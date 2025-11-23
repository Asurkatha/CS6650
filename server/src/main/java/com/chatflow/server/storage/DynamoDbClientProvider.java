package com.chatflow.server.storage;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import java.time.Duration;

/**
 * Singleton provider for DynamoDbClient
 */
public final class DynamoDbClientProvider {

    private static volatile DynamoDbClient client;

    public static DynamoDbClient getClient() {
        if (client == null) {
            synchronized (DynamoDbClientProvider.class) {
                if (client == null) {
                    // REGION via env AWS_REGION or default here
                    Region region = Region.of(System.getenv().getOrDefault("AWS_REGION", "us-west-2"));

                    client = DynamoDbClient.builder()
                            .region(region)
                            .credentialsProvider(DefaultCredentialsProvider.create())
                            .httpClientBuilder(ApacheHttpClient.builder().socketTimeout(Duration.ofSeconds(30)))
                            .build();
                }
            }
        }
        return client;
    }

    private DynamoDbClientProvider() {}
}
