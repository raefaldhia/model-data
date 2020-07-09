package io.github.raefaldhia.repository;

import java.time.Year;
import java.util.List;

import io.github.raefaldhia.model.Document;

public interface IRepository {
    public void
    delete (final Document document);

    public void
    delete (final String id);

    public List<Document>
    filter (final String author,
            final String name,
            final Integer year,
            final List<String> words);

    public Document
    get (final String id);

    public void
    save (final Document document);
}