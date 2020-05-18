package io.github.raefaldhia.dao;

import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;

import org.apache.hadoop.hbase.TableName;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.types.ObjectId;

import io.github.raefaldhia.database.Connection;
import io.github.raefaldhia.database.HBase.HBaseConnection;
import io.github.raefaldhia.model.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.MongoClient;

public class DocumentDAO {
    private static final DocumentDAO instance = new DocumentDAO();

    private static final String TABLE_NAME = "document";

    private static final String DATABASE_NAME = "database";

    private DocumentDAO() {

    }

    public void clear() {
        final MongoClient connection = Connection.GetInstance().getConnection();

        connection.getDatabase(DATABASE_NAME).getCollection(TABLE_NAME).drop();

        connection.close();
    }

    public void delete(final Document document) {
        this.delete(document.getId());
    }

    public void delete(final ObjectId documentId) {
        final MongoClient connection = Connection.GetInstance().getConnection();

        connection.getDatabase(DATABASE_NAME).getCollection(TABLE_NAME, Document.class)
                .deleteOne(Filters.eq("_id", documentId));

        connection.close();
    }

    public Document get(final ObjectId _id) {
        final MongoClient connection = Connection.GetInstance().getConnection();

        final Document result = connection.getDatabase(DATABASE_NAME).getCollection(TABLE_NAME, Document.class)
                .find(Filters.eq("_id", _id)).first();

        connection.close();

        return result;
    }

    public List<Document> getAll() {
        final MongoClient connection = Connection.GetInstance().getConnection();

        final List<Document> result = new ArrayList<Document>();

        connection.getDatabase(DATABASE_NAME).getCollection(TABLE_NAME, Document.class).find().into(result);

        connection.close();

        return result;
    }

    public void put(final Document document) throws IOException {
        HBaseConnection.Create((connection) -> {
            connection.getTable(TableName.valueOf("arg0"), (table) -> {
                Put put = new Put(Bytes.toBytes(student.getRow()));

            put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes(student.getName()));
            put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes(student.getCity()));
            put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("designation"), Bytes.toBytes(student.getDesignation()));
            put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("salary"), Bytes.toBytes(student.getSalary()));

            table.put(put);
            });
        });
        final MongoClient 
        connection = Connection
            .GetInstance()
            .getConnection();

        if (document.getId() == null) {
            document.setId(new ObjectId());
        }
    
        connection
            .getDatabase(DATABASE_NAME)
            .getCollection(TABLE_NAME, Document.class)
            .replaceOne(Filters.eq("_id", document.getId()), document, (new ReplaceOptions()).upsert(true));

        connection.close();
    }

    public static DocumentDAO GetInstance () {
        return instance;
    }
}