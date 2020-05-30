package io.github.raefaldhia.database;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class Connection {
    private static final Connection
    instance = new Connection();

    public MongoClient
    getConnection() {
        return MongoClients.create("mongodb://root:example@mongodb/?authSource=admin");
    }

    public static Connection
    GetInstance() {
        return instance;
    }
}