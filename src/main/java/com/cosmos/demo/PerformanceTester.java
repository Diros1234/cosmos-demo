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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * This class allows testing the performance by loading data with multiple threads.
 */
public class PerformanceTester {

    public static final String ACCOUNT = Config.getInstance().getString("cosmos.account");
    public static final String COLLECTION = Config.getInstance().getString("cosmos.collection");
    public static final String SECRET = Config.getInstance().getString("cosmos.secret");

    private int failedCount = 0;

    public static void main(String[] args) throws InterruptedException {
        final PerformanceTester tester = new PerformanceTester();
        tester.run();
    }

    private void run() throws InterruptedException {
        final long executionsCount = 10_000;
        final long threadCount = 4;

        final ExecutorService executor = Executors.newFixedThreadPool(4);
        System.out.println(">> Starting");

        final MongoClientOptions.Builder builder = new MongoClientOptions.Builder()
            .maxConnectionIdleTime(0)
//            .writeConcern(WriteConcern.ACKNOWLEDGED)
            ;

        try (MongoClient mongoClient = new MongoClient(new MongoClientURI(buildConnectionUrl(ACCOUNT, SECRET), builder))) {
            // Get collection
            final MongoDatabase database = mongoClient.getDatabase(ACCOUNT);
            final MongoCollection<Document> collection = database.getCollection(COLLECTION);

            final long initialCount = collection.count();

            final Runnable task = () -> {
                final Document document = buildMongoDocument();
                try {
                    collection.insertOne(document);
//                    System.out.println("Inserted from:" + Thread.currentThread().getId());
                } catch (Exception e) {
                    increaseFaildedCount();
                    System.out.println("ERROR: " + e.getMessage());
                }
            };

            long init = System.currentTimeMillis();
            for (long i = 0; i < executionsCount; i++) {
                if (threadCount == 1)
                    task.run();
                else
                    executor.submit(task);
            }
            executor.shutdown();

            System.out.println(format("Threads terminated successfully? %s", executor.awaitTermination(60, TimeUnit.MINUTES)));
            final long executionTime = System.currentTimeMillis() - init;
            System.out.println(format("Initial count: %s", initialCount));
            final long finalCount = collection.count();
            System.out.println(format("Final count: %s", finalCount));
            System.out.println("Execution time (ms):\t" + executionTime);
            System.out.println("Ratio (inserts/sec):\t" + Double.valueOf(finalCount - initialCount) / Double.valueOf(executionTime / 1000));
            System.out.println(format("Created: %s", finalCount - initialCount));
            System.out.println(format("Failed: %s", failedCount));
            System.out.println(format("Does count match? %s", (finalCount - initialCount) == executionsCount));
        }
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
        final String[] BOOKS = new String[]{
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
        };
        return BOOKS[ThreadLocalRandom.current().nextInt(BOOKS.length)];
    }

    private String buildConnectionUrl(String account, String key) {
        return String.format("mongodb://%s:%s@%s.documents.azure.com:10255/?ssl=true&replicaSet=globaldb", account, key, account);
    }

    private String randomAuthor() {
        final String[] AUTHORS = new String[]{
            "Neal Stephenson",
            "J. R. R. Tolkien",
            "Dan Simmons",
            "Richard Morgan",
            "Alfred Bester",
            "Connie Willis",
            "William Gibson"
        };
        return AUTHORS[ThreadLocalRandom.current().nextInt(AUTHORS.length)];
    }

    private String randomContentType() {
        final String[] CONTENT_TYPES = new String[]{
            "application/json",
            "application/pdf",
            "application/vnd.ms-excel",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        };
        return CONTENT_TYPES[ThreadLocalRandom.current().nextInt(CONTENT_TYPES.length)];
    }

    // TODO implement without blocking
    synchronized void increaseFaildedCount() {
        failedCount++;
    }

}
