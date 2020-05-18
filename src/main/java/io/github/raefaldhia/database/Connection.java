package io.github.raefaldhia.database;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.pojo.PojoCodecProvider;

public class Connection {
    private static final Connection
    instance = new Connection();

    private static final ServerAddress
    serverAddress = new ServerAddress("mongo", 27017);

    private static final MongoCredential
    credential = MongoCredential
        .createCredential("root", "admin", "example".toCharArray());

    private static final MongoClientOptions
    clientOptions = MongoClientOptions
        .builder()
        .codecRegistry(
            CodecRegistries.fromRegistries(
                MongoClientSettings.getDefaultCodecRegistry(),
                CodecRegistries.fromProviders(
                    PojoCodecProvider
                        .builder()
                        .automatic(true)
                        .build())))
        .build();
    
    public MongoClient getConnection() {
        return new MongoClient(serverAddress, credential, clientOptions);
    }

    public static Connection
    GetInstance() {
        return instance;
    }
}