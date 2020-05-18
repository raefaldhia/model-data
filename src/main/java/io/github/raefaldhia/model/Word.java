package io.github.raefaldhia.model;

import java.util.Set;

import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.bson.codecs.pojo.annotations.BsonProperty;

@BsonDiscriminator
public class Word {
	private final String word;

    private final Set<WordDocument> documents;

	@BsonCreator
	public Word (
        @BsonProperty("word") final String word, 
        @BsonProperty("documents") final Set<WordDocument> documents)  {
        this.word = word;

        this.documents = documents;
    }

    @BsonId
	public String getWord () {
		return word;
	}

    public Set<WordDocument> getDocuments () {
        return documents;
    }

    @BsonIgnore
    @Override
    public String toString() {
        String string = this.getWord() + "\n";

        for (WordDocument document : this.getDocuments()){
            string = string + "    " + document.getDocumentId() + ", " + document.getFrequency() + "\n";
        }

        return string;
    }
}