package io.github.raefaldhia.dao;

import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;

import io.github.raefaldhia.database.Connection;
import io.github.raefaldhia.model.Word;

public class WordDAO {
    private static final WordDAO
    instance = new WordDAO();

    private static final String
    TABLE_NAME = "word";

    private static final String
    DATABASE_NAME = "database";

    private WordDAO () {
        
    }

    public void clear () {
        final MongoClient
        connection = Connection
            .GetInstance()
            .getConnection();

        connection
            .getDatabase(DATABASE_NAME)
            .getCollection(TABLE_NAME)
            .drop();

        connection.close();
    }

    public void delete (final Word word) {
        this.delete(word.getWord());
    }

    public void delete (final String _id) {
        final MongoClient
        connection = Connection
            .GetInstance()
            .getConnection();

        connection
            .getDatabase(DATABASE_NAME)
            .getCollection(TABLE_NAME, Word.class)
            .deleteOne(Filters.eq("_id", _id));

        connection.close();
    }

    public Word get (final String _id) {
        final MongoClient
        connection = Connection
            .GetInstance()
            .getConnection();

        final Word
        result = connection
            .getDatabase(DATABASE_NAME)
            .getCollection(TABLE_NAME, Word.class)
            .find(Filters.eq("_id", _id))
            .first();

        connection.close();

        return result;
    }

    public void put (final Word word) {
        final MongoClient 
        connection = Connection
            .GetInstance()
            .getConnection();

        connection
            .getDatabase(DATABASE_NAME)
            .getCollection(TABLE_NAME, Word.class)
            .replaceOne(
                Filters.eq("_id", word.getWord()),
                word, 
                (new ReplaceOptions()).upsert(true));

        connection.close();
    }

    public static WordDAO GetInstance () {
        return instance;
    }    
}