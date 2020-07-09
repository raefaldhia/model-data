package io.github.raefaldhia.repository;

import java.time.Year;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.types.ObjectId;

import io.github.raefaldhia.model.Document;

public class MongoDBDocumentRepository implements IRepository {
    private static final CodecRegistry
    codecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                                                   CodecRegistries.fromProviders(PojoCodecProvider.builder()
                                                                                                  .automatic(true)
                                                                                                  .build()));

    private static final MongoCredential
    credential = MongoCredential.createCredential("mongoadmin", "admin", "secret".toCharArray());

    private static final MongoClientSettings
    clientSettings = MongoClientSettings.builder()
                                        .codecRegistry(codecRegistry)
                                        .credential(credential)
                                        .applyToClusterSettings(builder -> {
                                            builder.hosts(Arrays.asList(new ServerAddress("localhost", 27017)));
                                        })
                                        .build();

    private static final MongoClient
    client = MongoClients.create(clientSettings);

    private static final MongoDatabase
    database = client.getDatabase("database");

    public
    MongoDBDocumentRepository() {
        database.getCollection("documents_index").createIndex(Indexes.ascending("author", "documentId"));
        database.getCollection("documents_index").createIndex(Indexes.ascending("name", "documentId"));
        database.getCollection("documents_index").createIndex(Indexes.ascending("year", "documentId"));
        database.getCollection("documents_index").createIndex(Indexes.ascending("word", "documentId"));
    }

    public void
    delete (final Document document) {
        if (document == null) {
            return;
        }

        database.getCollection("documents")
                .deleteOne(Filters.eq("_id", document.getId()));

        database.getCollection("documents_index")
                .deleteMany(Filters.and(Filters.eq("documentId", document.getId()),
                                        Filters.or(Filters.eq("author", document.getAuthor()),
                                                   Filters.eq("name", document.getName()),
                                                   Filters.eq("year", document.getYear()))));

        document.getWords()
                .forEach(word -> {
                    database.getCollection("documents_index")
                            .deleteMany(Filters.and(Filters.eq("word", word.getName()),
                                                    Filters.eq("documentId", document.getId())));
                });
    }

    public void
    delete (final String id) {
        if (id == null) {
            return;
        }

        this.delete(this.get(id));
    }

    public List<Document>
    filter (final String author,
            final String name,
            final Integer year,
            final List<String> words) {
        return Optional.ofNullable(Arrays.asList(Optional.ofNullable(author)
                                                         .map(author0 -> this.getIdsByAuthor(author))
                                                         .orElse(null),
                                                 Optional.ofNullable(name)
                                                         .map(name0 -> this.getIdsByName(name0))
                                                         .orElse(null),
                                                 Optional.ofNullable(year)
                                                         .map(year0 -> this.getIdsByYear(year0))
                                                         .orElse(null),
                                                 Optional.ofNullable(words)
                                                         .map(words0 -> this.getIdsByWords(words0))
                                                         .orElse(null))
                                         .stream()
                                         .filter(set0 -> set0 != null)
                                         .reduce((Set<String>) null, (set0, set1) -> this.joinIndex(set0, set1)))
                       .orElse(new HashSet<String>())
                       .stream()
                       .map(id0-> get(id0))
                       .filter(arg0 -> arg0 != null)
                       .collect(Collectors.toList());
    }

    public Document
    get (final String id) {
        if (id == null) {
            return null;
        }

        return database.getCollection("documents", Document.class)
                       .find(Filters.eq("_id", new ObjectId(id)))
                       .first();
    }

    public Set<String>
    getIdsByAuthor (final String author) {
        return Optional.ofNullable(database.getCollection("documents_index")
                                           .aggregate(Arrays.asList(Aggregates.match(Filters.eq("author", author)),
                                                                    Aggregates.group(null, Arrays.asList(Accumulators.addToSet("documents", "$documentId")))))
                                           .first())
                       .map(documents -> documents.get("documents", new ArrayList<ObjectId>())
                                                  .stream()
                                                  .map(objectId -> objectId.toString())
                                                  .collect(Collectors.toSet()))
                       .orElse(new HashSet<String>());
    }

    private Set<String>
    getIdsByName (final String name) {
        return Optional.ofNullable(database.getCollection("documents_index")
                                           .aggregate(Arrays.asList(Aggregates.match(Filters.eq("name", name)),
                                                                    Aggregates.group(null, Arrays.asList(Accumulators.addToSet("documents", "$documentId")))))
                                           .first())
                       .map(documents -> documents.get("documents", new ArrayList<ObjectId>())
                                                  .stream()
                                                  .map(objectId -> objectId.toString())
                                                  .collect(Collectors.toSet()))
                       .orElse(new HashSet<String>());
    }

    public Set<String>
    getIdsByYear (final int year) {
        return Optional.ofNullable(database.getCollection("documents_index")
                                           .aggregate(Arrays.asList(Aggregates.match(Filters.eq("year", year)),
                                                                    Aggregates.group(null, Arrays.asList(Accumulators.addToSet("documents", "$documentId")))))
                                           .first())
                       .map(documents -> documents.get("documents", new ArrayList<ObjectId>())
                                                  .stream()
                                                  .map(objectId -> objectId.toString())
                                                  .collect(Collectors.toSet()))
                       .orElse(new HashSet<String>());
    }

    public Set<String>
    getIdsByWords (final List<String> wordsQuery) {
        return Optional.ofNullable(database.getCollection("documents_index")
                                           .aggregate(Arrays.asList(Aggregates.match(Filters.in("word", wordsQuery)),
                                                                    Aggregates.group("$word", Arrays.asList(Accumulators.addToSet("documents", "$documentId"))),
                                                                    Aggregates.group(null, Arrays.asList(Accumulators.addToSet("documents", "$documents")))))
                                           .first())
                       .map(documents -> documents.get("documents", new ArrayList<List<ObjectId>>())
                                                  .stream()
                                                  .map(list -> list.stream()
                                                                   .map(objectId -> objectId.toString())
                                                                   .collect(Collectors.toSet()))
                                                  .reduce((set0, set1) -> joinIndex(set0, set1))
                                                  .orElse(new HashSet<String>()))
                       .orElse(new HashSet<String>());
    }

    public void
    save (final Document document) {
        if (document.getId() == null) {
            document.setId(new ObjectId());
        }

        database.getCollection("documents", Document.class)
                .insertOne(document);

        final MongoCollection<org.bson.Document>
        indexCollection = database.getCollection("documents_index");

        indexCollection.insertOne(new org.bson.Document()
                                              .append("documentId", document.getId())
                                              .append("author", document.getAuthor()));

        indexCollection.insertOne(new org.bson.Document()
                                              .append("documentId", document.getId())
                                              .append("name", document.getName()));

        indexCollection.insertOne(new org.bson.Document()
                                              .append("documentId", document.getId())
                                              .append("year", document.getYear()));

        document.getWords()
                .forEach(word -> {
                    indexCollection.insertOne(new org.bson.Document()
                                                          .append("documentId", document.getId())
                                                          .append("word", word.getName()));
                });
    }

    private Set<String>
    joinIndex (final Set<String> set0,
               final Set<String> set1) {
        return Optional.ofNullable(set0)
                       .map(set0NotNull -> {
                           set0NotNull.retainAll(set1);

                           return set0NotNull;
                       })
                       .orElse(set1);
    }
}