package io.github.raefaldhia.model;

import java.time.Year;
import java.util.ArrayList;
import java.util.List;

import org.bson.codecs.pojo.annotations.BsonDiscriminator;
import org.bson.types.ObjectId;

@BsonDiscriminator
public class Document {
    private ObjectId
    id;

    private String
    author;

    private String
    name;

    private int
    year;

    private List<Word>
    words;

    public
    Document() {
        this(new ObjectId());
    }

    public
    Document(final ObjectId id) {
        this.id = id;
        this.words = new ArrayList<Word>();
    }

    public ObjectId
    getId() {
        return id;
    }

    public void
    setId(final ObjectId id) {
        this.id = id;
    }

    public String
    getAuthor() {
        return author;
    }

    public void
    setAuthor(final String author) {
        this.author = author;
    }

    public String
    getName() {
        return name;
    }

    public void
    setName(final String name) {
        this.name = name;
    }

    public int
    getYear() {
        return year;
    }

    public void
    setYear(final int year) {
        this.year = year;
    }

    public List<Word>
    getWords() {
        return words;
    }

    public void
    setWords(List<Word> words) {
        this.words = words;
    }

    public static class Word {
        private String
        name;

        private int
        frequency;

        public
        Word() { }

        public
        Word(final String name,
             final int frequency) {
            this.name = name;
            this.frequency = frequency;
        }

        public String
        getName() {
            return name;
        }

        public void
        setName(String name) {
            this.name = name;
        }

        public int
        getFrequency() {
            return frequency;
        }

        public void
        setFrequency(int frequency) {
            this.frequency = frequency;
        }
    }
}