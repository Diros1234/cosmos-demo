package com.cosmos.demo;

import com.cosmos.demo.config.Config;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class CosmosConnectionTester {

    public static final Config config = Config.getInstance();

    public static final long PERIOD_SECONDS = 30 * 60;
    private static final long KEEP_ALIVE_PERIOD = 10;
    public static final int MAX_CONNECTION_IDLE_TIME = 0;

    // CosmosDB configuration
    private String containerName = config.getString("containerName");
    private String collectionName = config.getString("collectionName");
    private String username = config.getString("username");
    public static final String secret = config.getString("secret");

    // Custom connection handler
    private BucketHelper<MongoCollection> buckets;

    public static void main(String[] args) {

        final CosmosConnectionTester proc = new CosmosConnectionTester().start();

        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        scheduler.scheduleAtFixedRate(() -> {
            try {
                proc.findDomains();
            } catch (MongoSocketException e) {
                System.out.println("MongoSocketException");
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Exception :");
            }
        }, 0l, PERIOD_SECONDS, TimeUnit.SECONDS);
//        scheduler.scheduleAtFixedRate(() -> proc.keepAlive(), 0l, KEEP_ALIVE_PERIOD, TimeUnit.SECONDS);
    }

    private CosmosConnectionTester start() {
        buckets = createBuckets();
        return this;
    }

    private void findDomains() {
        final List<Document> results = new ArrayList<>();

        buckets.bucket()
            .find(Filters.eq("_type", "Domain"))
            .into(results).stream();

        System.out.println(format("[%s] Domains found: %s -> %s", new Date(), results.size(), results.stream()
            .map(d -> d.getString("name"))
            .collect(Collectors.joining(", ", "[", "]"))));
    }

    private BucketHelper<MongoCollection> createBuckets() {
        return new BucketHelper<>(1, BucketHelper.STATUS_OK, () -> {

            final String connectionString = generateConnectionURL(
                containerName,
                username,
                secret);

            final MongoClient mc = new MongoClient(
                new MongoClientURI(
                    connectionString,
                    MongoClientOptions.builder()
                        .serverSelectionTimeout(5000)
                        // Experimental: may serve as workaround?
                        .maxConnectionIdleTime(MAX_CONNECTION_IDLE_TIME)
                )
            );
            final MongoDatabase database = mc.getDatabase(username);
            // check database because getCollection returns OK even when connections is not available
            database
                .withReadPreference(ReadPreference.nearest())
                .runCommand(new Document("serverStatus", 1));

            final MongoCollection collection = database
                .getCollection(collectionName);
            System.out.println(format("MongoDB connection OK: " + containerName));
            return collection;
        });
    }

    private String generateConnectionURL(String container, String username, String password) {
        return format("mongodb://%s:%s@%s:10255/?ssl=true&replicaSet=globaldb", username, password, container);
    }

    private void keepAlive() {
        System.out.println(format("[%s] KeepAlive: %s",
            new Date(), buckets.bucket().count(Filters.eq("_type", "Domain"))));
    }

}
