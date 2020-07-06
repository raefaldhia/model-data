package io.github.raefaldhia.repository;

import java.io.IOException;
import java.time.Year;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.bson.types.ObjectId;

import io.github.raefaldhia.model.Document;
import io.github.raefaldhia.model.Document.Word;

public class HBaseDocumentRepository {
    private static final Configuration
    config = HBaseConfiguration.create();

    public static void
    delete (final Document
            document) throws IOException {
        if (document == null) {
            return;
        }

        final Connection
        connection = ConnectionFactory.createConnection(config);
        {
            // Database
            {
                final Table
                table = connection.getTable(TableName.valueOf("documents"));
                {
                    table.delete(new Delete(Bytes.toBytes(document.getId().toString())));
                }
                table.close();
            }
            // Index
            {
                final Table
                table = connection.getTable(TableName.valueOf("documents_index"));
                {
                    {
                        final Delete
                        delete = new Delete(Bytes.add(Bytes.toBytes("author_"), Bytes.toBytes(document.getAuthor())));
                        delete.addColumn(Bytes.toBytes("documents"), Bytes.toBytes(document.getId().toString()));

                        table.delete(delete);
                    }
                    {
                        final Delete
                        delete = new Delete(Bytes.add(Bytes.toBytes("name_"), Bytes.toBytes(document.getName())));
                        delete.addColumn(Bytes.toBytes("documents"), Bytes.toBytes(document.getId().toString()));

                        table.delete(delete);
                    }
                    {
                        final Delete
                        delete = new Delete(Bytes.add(Bytes.toBytes("year_"), Bytes.toBytes(document.getYear().getValue())));
                        delete.addColumn(Bytes.toBytes("documents"), Bytes.toBytes(document.getId().toString()));

                        table.delete(delete);
                    }
                    for (final Document.Word
                         word : document.getWords()) {
                        final Delete
                        delete = new Delete(Bytes.add(Bytes.toBytes("word_"), Bytes.toBytes(word.getName())));
                        delete.addColumn(Bytes.toBytes("documents"), Bytes.toBytes(document.getId().toString()));

                        table.delete(delete);
                    }
                }
                table.close();
            }
        }
        connection.close();
    }

    public static void
    delete (final String
            id) throws IOException {
        if (id == null) {
            return;
        }

        HBaseDocumentRepository.delete(HBaseDocumentRepository.get(id));
    }

    public static List<Document>
    filter (final String
            author,
            final String
            name,
            final Year
            year,
            final List<String>
            words) throws IOException {
        return Optional.ofNullable(Arrays.asList(HBaseDocumentRepository.getIdsByAuthor(Optional.ofNullable(author)
                                                                                           .map(author0 -> Bytes.toBytes(author))
                                                                                           .orElse(null)),
                                                 HBaseDocumentRepository.getIdsByName(Optional.ofNullable(name)
                                                                                         .map(name0 -> Bytes.toBytes(name0))
                                                                                         .orElse(null)),
                                                 HBaseDocumentRepository.getIdsByYear(Optional.ofNullable(year)
                                                                                         .map(year0 -> Bytes.toBytes(year0.getValue()))
                                                                                         .orElse(null)),
                                                 HBaseDocumentRepository.getIdsByWords(Optional.ofNullable(words)
                                                                                          .map(words0 -> words0.stream()
                                                                                                               .map(word -> Bytes.toBytes(word))
                                                                                                               .collect(Collectors.toList()))
                                                                                          .orElse(null)))
                       .stream()
                       .filter(set0 -> set0 != null)
                       .reduce((Set<String>) null, (set0, set1) -> HBaseDocumentRepository.joinIndex(set0, set1)))
                       .orElse(new HashSet<String>())
                       .stream()
                       .map(ThrowingFunction.unchecked(id0-> get(id0)))
                       .filter(arg0 -> arg0 != null)
                       .collect(Collectors.toList());
    }

    public static Document
    get (final String
         id) throws IOException {
        if (id == null) {
            return null;
        }

        Document
        document = null;

        final Connection
        connection = ConnectionFactory.createConnection(config);
        {
            final Table
            table = connection.getTable(TableName.valueOf("documents"));
            {
                final FilterList
                filter = new FilterList();
                filter.addFilter(new RowFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(id))));

                final Scan
                scan = new Scan();
                scan.setFilter(filter);

                final ResultScanner
                resultScanner = table.getScanner(scan);
                {
                    final Result
                    result = resultScanner.next();

                    if (result == null) {
                        return null;
                    }

                    document = new Document(new ObjectId(id));
                    document.setAuthor(Bytes.toString(result.getValue(Bytes.toBytes("information"), Bytes.toBytes("author"))));
                    document.setName(Bytes.toString(result.getValue(Bytes.toBytes("information"), Bytes.toBytes("name"))));
                    document.setYear(Year.of(Bytes.toInt(result.getValue(Bytes.toBytes("information"), Bytes.toBytes("year")))));

                    for (final byte[]
                         word : result.getFamilyMap(Bytes.toBytes("words")).keySet()) {
                        final Document.Word
                        newWord = new Document.Word();
                        newWord.setName(Bytes.toString(word));
                        newWord.setFrequency(Bytes.toInt(result.getValue(Bytes.toBytes("words"), word)));

                        document.getWords().add(newWord);
                    }
                }
                resultScanner.close();
            }
            table.close();
        }
        connection.close();

        return document;
    }

    private static Set<String>
    getIdsByAuthor (final byte[]
                    author) throws IOException {
        return getIdsByIndex(Bytes.toBytes("author_"), Arrays.asList(author));
    }

    private static Set<String>
    getIdsByName (final byte[]
                  name) throws IOException {
        return getIdsByIndex(Bytes.toBytes("name_"), Arrays.asList(name));
    }

    private static Set<String>
    getIdsByYear (final byte[]
                  year) throws IOException {
        return getIdsByIndex(Bytes.toBytes("year_"), Arrays.asList(year));
    }

    private static Set<String>
    getIdsByWords (final List<byte[]>
                   wordsQuery) throws IOException {
        return getIdsByIndex(Bytes.toBytes("word_"), wordsQuery);
    }

    private static Set<String>
    getIdsByIndex(final byte[] 
                  prefix,
                  final List<byte[]>
                  indexes) {
        return Optional.ofNullable(indexes)
                       .map(indexesNotNull -> indexesNotNull.stream()
                                                            .filter(index -> index != null)
                                                            .collect(Collectors.toList()))
                       .map(ThrowingFunction.unchecked(indexesNotNull -> indexesNotNull.size() == 0 ? null
                                                                                                    : HBaseDocumentRepository.getIdsByFilterList(getIdsFilterList(prefix, indexesNotNull))))
                       .orElse(null);
    }

    private static Set<String>
    getIdsByFilterList (final FilterList
                        filter) throws IOException {
        Set<String>
        result = null;

        final Connection
        connection = ConnectionFactory.createConnection(config);
        {
            final Table
            table = connection.getTable(TableName.valueOf("documents_index"));
            {
                final Scan
                scan = new Scan();
                scan.setFilter(filter);

                final ResultScanner
                resultScanner = table.getScanner(scan);
                {
                    result = joinResultIndex(resultScanner);
                }
                resultScanner.close();
            }
            table.close();
        }
        connection.close();

        return result;
    }

    private static FilterList
    getIdsFilterList(final byte[]
                     prefix,
                    final List<byte[]>
                    indexes) {
        final FilterList
        filter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        filter.addFilter(indexes.stream()
                                .filter(index -> index != null)
                                .map(index -> HBaseDocumentRepository.getIdsRowFilter(prefix, index))
                                .collect(Collectors.toList()));

        return filter;
    }

    private static RowFilter
    getIdsRowFilter(final byte[]
                    prefix, 
                    final byte[]
                    index) {
        return new RowFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.add(prefix, index)));
    }

    public static void
    save (final Document
          document) throws IOException {
        if (document.getId() == null) {
            document.setId(new ObjectId());
        }

        final Connection
        connection = ConnectionFactory.createConnection(config);
        {
            // Database
            {
                final Table
                table = connection.getTable(TableName.valueOf("documents"));
                {
                    final Put
                    put = new Put(Bytes.toBytes(document.getId().toString()));
                    put.addColumn(Bytes.toBytes("information"), Bytes.toBytes("author"), Bytes.toBytes(document.getAuthor()));
                    put.addColumn(Bytes.toBytes("information"), Bytes.toBytes("name"), Bytes.toBytes(document.getName()));
                    put.addColumn(Bytes.toBytes("information"), Bytes.toBytes("year"), Bytes.toBytes(document.getYear().getValue()));

                    document.getWords().forEach(word -> {
                        put.addColumn(Bytes.toBytes("words"), Bytes.toBytes(word.getName()), Bytes.toBytes(word.getFrequency()));
                    });

                    table.put(put);
                }
                table.close();
            }
            // Index
            {
                final Table
                table = connection.getTable(TableName.valueOf("documents_index"));
                {
                    {
                        final Put
                        put = new Put(Bytes.add(Bytes.toBytes("author_"), Bytes.toBytes(document.getAuthor())));
                        put.addColumn(Bytes.toBytes("documents"), Bytes.toBytes(document.getId().toString()), null);

                        table.put(put);
                    }
                    {
                        final Put
                        put = new Put(Bytes.add(Bytes.toBytes("name_"), Bytes.toBytes(document.getName())));
                        put.addColumn(Bytes.toBytes("documents"), Bytes.toBytes(document.getId().toString()), null);

                        table.put(put);
                    }
                    {
                        final Put
                        put = new Put(Bytes.add(Bytes.toBytes("year_"), Bytes.toBytes(document.getYear().getValue())));
                        put.addColumn(Bytes.toBytes("documents"), Bytes.toBytes(document.getId().toString()), null);

                        table.put(put);
                    }
                    for (final Word
                         word : document.getWords()) {
                        final Put
                        put = new Put(Bytes.add(Bytes.toBytes("word_"), Bytes.toBytes(word.getName())));
                        put.addColumn(Bytes.toBytes("documents"), Bytes.toBytes(document.getId().toString()), null);

                        table.put(put);
                    }
                }
                table.close();
            }
        }
        connection.close();
    }

    private static Set<String>
    joinResultIndex (final ResultScanner
                     scanner) {
        return Optional.ofNullable(StreamSupport.stream(scanner.spliterator(), false)
                                                .reduce((Set<String>) null,
                                                        (set0, set1) -> joinIndex(set0, set1.getFamilyMap(Bytes.toBytes("documents"))
                                                                                            .keySet()
                                                                                            .stream()
                                                                                            .map(id -> Bytes.toString(id))
                                                                                            .collect(Collectors.toSet())),
                                                        (set0, set1) -> joinIndex(set0, set1)))
                                                .orElse(new HashSet<String>());
    }

    private static Set<String>
    joinIndex (final Set<String>
               set0,
               final Set<String>
               set1) {
        return Optional.ofNullable(set0)
                       .map(set0NotNull -> {
                           set0NotNull.retainAll(set1);

                           return set0NotNull;
                       })
                       .orElse(set1);
    }

    @FunctionalInterface
    public interface ThrowingFunction<T, R, E extends Throwable> {
        R apply(T t) throws E;

        static <T, R, E extends Throwable> Function<T, R> 
        unchecked(ThrowingFunction<T, R, E> 
                  f) {
            return t -> {
                try {
                    return f.apply(t);
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            };
        }
    }
}