package io.github.raefaldhia.dao;

import com.mongodb.client.MongoDatabase;

import io.github.raefaldhia.database.Connection;

import com.mongodb.client.MongoClient;

public class Database {
    private static final Database 
    instance = new Database();

    private static final String
    DATABASE_NAME = "database";

    private Database() {

    }

    public void clear() {
        this.getDatabase((db) -> {
            db.drop();
        });
    }

    public interface cb_getDatabase {
        void exec(MongoDatabase database);
    }

    public void getDatabase(cb_getDatabase cb) {
        final MongoClient
        connection = Connection.GetInstance()
                               .getConnection();
        cb.exec(connection.getDatabase(DATABASE_NAME));
        connection.close();
    }

    public static Database GetInstance () {
        return instance;
    }
}