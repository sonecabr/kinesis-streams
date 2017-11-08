package com.soneca.pocs.kinesis.streams.ingest;


import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class PutRecords {

    public static void main(String[] args) {
        AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();

        Properties properties = new Properties();
        try {
            properties.load(
                    PutRecords
                            .class
                            .getClassLoader()
                            .getResourceAsStream("application.properties")
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
        clientBuilder.setRegion(properties.getProperty("amazonaws.kinesis.region"));
        clientBuilder.setCredentials(new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                return new BasicAWSCredentials(
                        properties.getProperty("amazonaws.user.key"),
                        properties.getProperty("amazonaws.user.secret")
                );
            }

            @Override
            public void refresh() {

            }
        });
        clientBuilder.setClientConfiguration(new ClientConfiguration());

        AmazonKinesis kinesisClient = clientBuilder.build();

        PutRecordsRequest putRecordsRequest  = new PutRecordsRequest();
        putRecordsRequest.setStreamName(properties.getProperty("amasonaws.kinesis.stream.name"));
        List<PutRecordsRequestEntry> putRecordsRequestEntryList  = new ArrayList();
        for (int i = 0; i < 100; i++) {
            String id = UUID.randomUUID().toString();
            PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(
                    ByteBuffer.wrap(String.format("{\"id\":\"%s\",\"key\":\"%s\"}", id, id).getBytes()));
            putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%s", id));
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }

        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        PutRecordsResult putRecordsResult  = kinesisClient.putRecords(putRecordsRequest);
        System.out.println("Put Result" + putRecordsResult);
    }
}
