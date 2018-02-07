package com.cosmos.demo;

import com.cosmos.demo.config.Config;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.lang.String.format;

/**
 * This class allows testing the performance by loading data with multiple threads.
 */
public class PerformanceTester {

    public static final String ACCOUNT = Config.getInstance().getString("cosmos.account");
    public static final String COLLECTION = Config.getInstance().getString("cosmos.collection");
    public static final String SECRET = Config.getInstance().getString("cosmos.secret");

    public static void main(String[] args) throws InterruptedException {
        final PerformanceTester tester = new PerformanceTester();
        tester.run();
    }

    private void run() throws InterruptedException {
        final long executionsCount = 1_000_000;
        final int threadCount = 32;

        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        System.out.println(">> Starting");

        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder()
            .maxConnectionIdleTime(0)
//            .writeConcern(WriteConcern.ACKNOWLEDGED)
            ;

        try (MongoClient mongoClient = new MongoClient(new MongoClientURI(buildConnectionUrl(ACCOUNT, SECRET), builder))) {
            // Get collection
            final MongoDatabase database = mongoClient.getDatabase(ACCOUNT);
            final MongoCollection<Document> collection = database.getCollection(COLLECTION);
            System.out.println("Connected to: " + collection.getNamespace());

            final long initialCount = collection.count();

            final List<Callable<Boolean>> callables = LongStream
                .rangeClosed(1, executionsCount)
                .mapToObj(i -> (Callable<Boolean>) () -> {
                    try {
                        collection.insertOne(buildMongoDocument());
                        return Boolean.TRUE;
                    } catch (Exception e) {
                        System.out.println("ERROR: " + e.getMessage());
                        return Boolean.FALSE;
                    }
                })
                .collect(Collectors.toList());

            System.out.println("Tasks initialized");

            long init = System.currentTimeMillis();
            final List<Future<Boolean>> results = executor.invokeAll(callables);
            executor.shutdown();

            System.out.println(format("Threads terminated successfully? %s", executor.awaitTermination(60, TimeUnit.MINUTES)));

            final Integer failedOperations = results.stream()
                .map(this::getBooleanAsInt)
                .reduce((v1, v2) -> v1 + v2)
                .get();
            final long successfulOperations = executionsCount - failedOperations;

            final long executionTime = System.currentTimeMillis() - init;
            System.out.println(format("Initial count: %s", initialCount));
            final long finalCount = collection.count();
            System.out.println(format("Final count: %s", finalCount));
            System.out.println("Execution time (ms):\t" + executionTime);
            System.out.println("Ratio (ops/sec):\t" + Double.valueOf(successfulOperations) / Double.valueOf(executionTime / 1000));
            System.out.println(format("Successful: %s", finalCount - initialCount));
            System.out.println(format("Failed: %s", failedOperations));
            System.out.println(format("Does count match? %s", successfulOperations == executionsCount));
        }
    }

    /**
     * true -> 0, false -> 1
     */
    private Integer getBooleanAsInt(Future<Boolean> r) {
        try {
            return r.get(60, TimeUnit.SECONDS) ? 0 : 1;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            System.out.println("Error getting value:");
            e.printStackTrace();
            return 0;
        }
    }

    private String buildConnectionUrl(String account, String key) {
        return String.format("mongodb://%s:%s@%s.documents.azure.com:10255/?ssl=true&replicaSet=globaldb", account, key, account);
    }

    private Document buildMongoDocument() {
        final Date dateCreated = new Date();
        final String author = randomAuthor();
        return new Document()
            .append("description", "Lorem Ipsum is simply dummy text of the printing and typesetting industry")
            .append("author", author)
            .append("dateCreated", dateCreated)
            .append("lastModifier", author)
            .append("dateModified", dateCreated)
            .append("currentVersion", "2.2")
            .append("content", Arrays.asList(
                createContentVersion(dateCreated, author, 1, 0),
                createContentVersion(dateCreated, author, 2, 0),
                createContentVersion(dateCreated, author, 2, 2)
                )
            )
            .append("storagecontainer", "some_container");
    }

    private Document createContentVersion(Date dateCreated, String author, Integer major, Integer minor) {
        return new Document()
            .append("name", radomFilename())
            .append("type", randomContentType())
            .append("size", new Long(10737418240l))
            .append("majorVersion", major)
            .append("minorVersion", minor)
            .append("author", author)
            .append("dateCreated", dateCreated)
            .append("lastModifier", author)
            .append("dateModified", dateCreated);
    }

    private String radomFilename() {
        return selectRandomValue(
            "The Diamond Age: or A Young Lady's Illustrated Primer",
            "Cryptonomicon",
            "Quicksilver",
            "The Confusion",
            "The System of the World",
            "Anathem",
            "Reamde",
            "Seveneves",
            "Hyperion",
            "The Fall of Hyperion",
            "Endymion",
            "The Rise of Endymion",
            "The Stars My Destination",
            "Doomsday Book"
        );
    }

    private String randomAuthor() {
        return selectRandomValue(
            "Neal Stephenson",
            "J. R. R. Tolkien",
            "Dan Simmons",
            "Richard Morgan",
            "Alfred Bester",
            "Connie Willis",
            "William Gibson"
        );
    }

    private String randomContentType() {
        return selectRandomValue(
            "application/json",
            "application/pdf",
            "application/vnd.ms-excel",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        );
    }

    private String selectRandomValue(String... words) {
        return words[ThreadLocalRandom.current().nextInt(words.length)];
    }

}
