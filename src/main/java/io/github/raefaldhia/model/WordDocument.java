package io.github.raefaldhia.model;

import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.bson.types.ObjectId;

@BsonDiscriminator
public class WordDocument {
    private ObjectId documentId;

    private int frequency;

    @BsonCreator
    public WordDocument (
        @BsonProperty("documentId") final ObjectId documentId, 
        @BsonProperty("frequency") final int frequency) {
        this.documentId = documentId;

        this.frequency = frequency;
    }

    @BsonId
    public ObjectId getDocumentId () {
        return documentId;
    }

    public int getFrequency () {
        return frequency;
    }
}