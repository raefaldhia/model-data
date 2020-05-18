package io.github.raefaldhia.model;

import com.google.gson.Gson;

import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.bson.types.ObjectId;

@BsonDiscriminator
public class Document {
    private ObjectId _id;

	private String name;

    private String year;

	private String author;

	@BsonCreator
	public Document (
        @BsonProperty("id") final ObjectId _id,
        @BsonProperty("name") final String name,
        @BsonProperty("year") final String year,
        @BsonProperty("author") final String author)  {
		this (name, year, author);
	
        this._id = _id;
	}

	public Document (
		final String name,
        final String year,
        final String author) {
        this.name = name;

        this.year = year;

        this.author = author;
	}

	public void setId (final ObjectId _id) {
		this._id = _id;
	}

	@BsonId
	public ObjectId getId() {
		return _id;
	}

	public String getAuthor () {
		return this.author;
	}

	public void setAuthor (final String author) {
		this.author = author;
	}

	public String getYear () {
		return this.year;
    }

	public void setYear (final String year) {
		this.year = year;
    }

    public String getName () {
        return name;
    }

    public void setName (final String name) {
        this.name = name;
	}

	@BsonIgnore
	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
}