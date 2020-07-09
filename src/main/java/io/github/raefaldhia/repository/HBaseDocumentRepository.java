package io.github.raefaldhia.repository;

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

public class HBaseDocumentRepository implements IRepository {
    private static final Configuration
    config = HBaseConfiguration.create();

    public 
    HBaseDocumentRepository () {}

    public void
    delete (final Document document) {
        try {
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
                            delete = new Delete(Bytes.add(Bytes.toBytes("year_"), Bytes.toBytes(document.getYear())));
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
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
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
                                                         .map(author0 -> this.getIdsByAuthor(Bytes.toBytes(author)))
                                                         .orElse(null),
                                                 Optional.ofNullable(name)
                                                         .map(name0 -> this.getIdsByName(Bytes.toBytes(name0)))
                                                         .orElse(null),
                                                 Optional.ofNullable(year)
                                                         .map(year0 -> this.getIdsByYear(Bytes.toBytes(year0)))
                                                         .orElse(null),
                                                 Optional.ofNullable(words)
                                                         .map(words0 -> words0.stream().map(word -> Bytes.toBytes(word))
                                                                                                         .collect(Collectors.toList()))
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
        try {
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
                        document.setYear(Bytes.toInt(result.getValue(Bytes.toBytes("information"), Bytes.toBytes("year"))));

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
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private Set<String>
    getIdsByAuthor (final byte[] author) {
        return getIdsByIndex(Bytes.toBytes("author_"), Arrays.asList(author));
    }

    private Set<String>
    getIdsByName (final byte[] name) {
        return getIdsByIndex(Bytes.toBytes("name_"), Arrays.asList(name));
    }

    private Set<String>
    getIdsByYear (final byte[] year) {
        return getIdsByIndex(Bytes.toBytes("year_"), Arrays.asList(year));
    }

    private Set<String>
    getIdsByWords (final List<byte[]> wordsQuery) {
        return getIdsByIndex(Bytes.toBytes("word_"), wordsQuery);
    }

    private Set<String>
    getIdsByIndex (final byte[] prefix,
                  final List<byte[]> indexes) {
        return Optional.ofNullable(indexes)
                       .map(indexesNotNull -> indexesNotNull.stream()
                                                            .filter(index -> index != null)
                                                            .collect(Collectors.toList()))
                       .map(indexesNotNull -> indexesNotNull.size() == 0 ? null
                                                                         : this.getIdsByFilterList(getIdsFilterList(prefix, indexesNotNull)))
                       .orElse(null);
    }

    private Set<String>
    getIdsByFilterList (final FilterList filter) {
        try {
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
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private FilterList
    getIdsFilterList (final byte[] prefix,
                    final List<byte[]> indexes) {
        final FilterList
        filter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        filter.addFilter(indexes.stream()
                                .filter(index -> index != null)
                                .map(index -> this.getIdsRowFilter(prefix, index))
                                .collect(Collectors.toList()));

        return filter;
    }

    private RowFilter
    getIdsRowFilter (final byte[] prefix, 
                    final byte[] index) {
        return new RowFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.add(prefix, index)));
    }

    public void
    save (final Document document) {
        try {
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
                        put.addColumn(Bytes.toBytes("information"), Bytes.toBytes("year"), Bytes.toBytes(document.getYear()));

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
                            put = new Put(Bytes.add(Bytes.toBytes("year_"), Bytes.toBytes(document.getYear())));
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
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private Set<String>
    joinResultIndex (final ResultScanner scanner) {
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