package io.github.raefaldhia;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.model.Filters;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.bson.Document;
import org.bson.types.ObjectId;

@WebServlet("/documents-hbase/*")
public class DocumentsControllerHBase extends HttpServlet {
    private static final long
    serialVersionUID = 1L;

    @Override
    protected void 
    doGet(final HttpServletRequest
          request,
          HttpServletResponse
          response) throws ServletException,
                           IOException {
        /*        if (request.getPathInfo() == null || 
            request.getPathInfo()
                   .equals("/")) {
            JsonArray
            documents = new JsonArray();
	    HBaseConnection.Create((connection) -> {
                connection.getTable(TableName.valueOf("documents"), (table) -> {
		    Scan
                    scan = new Scan();
		    scan.addFamily("information".getBytes());
		    FilterList
		    filter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		    if (request.getParameter("author") != null) {
			filter.addFilter(new SingleColumnValueFilter("information".getBytes(), "author".getBytes(), CompareOperator.EQUAL, Bytes.toBytes(request.getParameter("author"))));
		    }
   		    if (request.getParameter("name") != null) {
			filter.addFilter(new SingleColumnValueFilter("information".getBytes(), "name".getBytes(), CompareOperator.EQUAL, Bytes.toBytes(request.getParameter("name"))));
		    }
		    if (request.getParameter("year") != null) {
			filter.addFilter(new SingleColumnValueFilter("information".getBytes(), "year".getBytes(), CompareOperator.EQUAL, Bytes.toBytes(Integer.parseInt(request.getParameter("year")))));
		    }
                    scan.setFilter(filter);
		    ResultScanner
		    scanner = table.getScanner(scan);
		    for (Result
			 result = scanner.next();
			 result != null;
			 result = scanner.next()) {
			JsonObject
			document = new JsonObject();
			final byte[]
		        documentId = result.getRow();
			document.addProperty("id",
					     Bytes.toString(result.getRow()));
			document.addProperty("author",
					     Bytes.toString(result.getValue("information".getBytes(),
									    "author".getBytes())));
			document.addProperty("name",
					     Bytes.toString(result.getValue("information".getBytes(),
									    "name".getBytes())));
			document.addProperty("year",
					     Bytes.toInt(result.getValue("information".getBytes(),
							                 "year".getBytes())));
			connection.getTable(TableName.valueOf("words"), (wordsTable) -> {
			    JsonArray
			    words = new JsonArray();
			    Scan
                            wordsScan = new Scan();
	    		    FilterList
                            wordsFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
                            if (request.getParameter("words") != null) {
				for (String
				     wordString : request.getParameter("words")
					                 .split(" ")) {
				    wordsFilter.addFilter(new RowFilter(CompareOperator.EQUAL,
									new BinaryComparator(wordString.getBytes())));
				}
			    }
			    wordsScan.addColumn("frequency".getBytes(), documentId);
                            wordsScan.setFilter(wordsFilter);
			    ResultScanner
                            wordsScanner = wordsTable.getScanner(wordsScan);
			    for (Result
				 wordResult = wordsScanner.next();
				 wordResult != null;
				 wordResult = wordsScanner.next()) {
				JsonObject
				word = new JsonObject();
				word.addProperty("word",
						 Bytes.toString(wordResult.getRow()));
				word.addProperty("frequency",
						 Bytes.toInt(wordResult.getValue("frequency".getBytes(), documentId)));
				words.add(word);
			    }
			    document.add("words", words);
			});
			documents.add(document);
		    }
                    scanner.close();
		});
	    });
            sendAsJson(response, (new Gson().toJson(documents)));
            return;
            }*/
        if (request.getPathInfo() == null || 
            request.getPathInfo()
                   .equals("/")) {
            JsonArray
            documents = new JsonArray();
	    HBaseConnection.Create((connection) -> {
                final HashMap<String, Integer>
                documentsFilter = new HashMap<String, Integer>();
                connection.getTable(TableName.valueOf("documents"), (table) -> {
		    FilterList
		    filter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		    if (request.getParameter("author") != null) {
			filter.addFilter(new SingleColumnValueFilter("information".getBytes(), "author".getBytes(), CompareOperator.EQUAL, Bytes.toBytes(request.getParameter("author"))));
		    }
   		    if (request.getParameter("name") != null) {
			filter.addFilter(new SingleColumnValueFilter("information".getBytes(), "name".getBytes(), CompareOperator.EQUAL, Bytes.toBytes(request.getParameter("name"))));
		    }
		    if (request.getParameter("year") != null) {
			filter.addFilter(new SingleColumnValueFilter("information".getBytes(), "year".getBytes(), CompareOperator.EQUAL, Bytes.toBytes(Integer.parseInt(request.getParameter("year")))));
		    }
                    if (filter.size() > 0) {
                        Scan
                        scan = new Scan();
                        scan.addFamily("information".getBytes());
                        scan.setFilter(filter);
                        ResultScanner
                        scanner = table.getScanner(scan);
                        for (Result
                             result = scanner.next();
                             result != null;
                             result = scanner.next()) {
                            documentsFilter.put(Bytes.toString(result.getRow()), 0);
                        }
                        scanner.close();
                    }
		});
                connection.getTable(TableName.valueOf("words"), (wordsTable) -> {
                    Scan
                    wordsScan = new Scan();
                    FilterList
                    wordsFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
                    if (request.getParameter("words") != null) {
                        for (String
			     word : request.getParameter("words")
                                           .split(" ")) {
                            wordsFilter.addFilter(new RowFilter(CompareOperator.EQUAL,
                                                                new BinaryComparator(word.getBytes())));
                        }
                    }
                    FilterList
                    documentFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
                    for (String
                         document : documentsFilter.keySet()) {
                        documentFilter.addFilter(new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(document.getBytes())));
                    }
                    wordsScan.setFilter(new FilterList(wordsFilter, documentFilter));
                    ResultScanner
                    wordsScanner = wordsTable.getScanner(wordsScan);
                    boolean
                    prevOptimization = documentsFilter.size() > 0;
                    for (Result
                         wordResult = wordsScanner.next();
                         wordResult != null;
                         wordResult = wordsScanner.next()) {
                        Map<byte[], byte[]>
                        frequencies = wordResult.getFamilyMap("frequency".getBytes());
                        if (prevOptimization) {
                            for (byte[]
                                 key : frequencies.keySet()) {
                                String
                                keyString = Bytes.toString(key);
                                if (documentsFilter.get(keyString) != null) {
                                    documentsFilter.put(keyString, documentsFilter.get(keyString) + 1);
                                }
                            }
                        }
                        else {
                            for (byte[]
                                 key : frequencies.keySet()) {
                                String
                                keyString = Bytes.toString(key);
                                int old;
                                if (documentsFilter.get(keyString) == null) {
                                    old = 0;
                                } else {
                                    old = documentsFilter.get(keyString);
                                }
                                documentsFilter.put(keyString, old + 1);
                            }
                        }
                    }
                    if (request.getParameter("words") != null) {
                        for (String
                             document : documentsFilter.keySet()) {
                            if (documentsFilter.get(document) != request.getParameter("words")
                                                                        .split(" ")
                                                                        .length) {
                                documentsFilter.remove(document);
                            }
                        }
                    }
                    for (String
                         documentId : documentsFilter.keySet()) {
                        JsonObject
			document = new JsonObject();
			document.addProperty("id",
                                             documentId);
                        documents.add(document);
                    }
                });
            });
            sendAsJson(response, (new Gson().toJson(documents)));
            return;
        }
        response.sendError(HttpServletResponse.SC_BAD_REQUEST);
        return;
    }

    @Override
    protected void

    doPost(HttpServletRequest
           request, 
           HttpServletResponse
           response) throws ServletException, 
                            IOException {
        if ((request.getPathInfo() == null || 
             request.getPathInfo()
                    .equals("/")) && 
            request.getContentType()
                   .equals("application/json")) {
            final JsonElement
	    body = parseJson(request);
	    final String
            documentId = new ObjectId().toString();
	    HBaseConnection.Create((connection) -> {
                connection.getTable(TableName.valueOf("documents"), (table) -> {
                    Put
		    put = new Put(Bytes.toBytes(documentId));
		    put.addColumn(Bytes.toBytes("information"),
				  Bytes.toBytes("author"),
				  Bytes.toBytes(body.getAsJsonObject().get("author").getAsString()));
		    put.addColumn(Bytes.toBytes("information"),
				  Bytes.toBytes("name"),
				  Bytes.toBytes(body.getAsJsonObject().get("name").getAsString()));
		    put.addColumn(Bytes.toBytes("information"),
				  Bytes.toBytes("year"),
				  Bytes.toBytes(body.getAsJsonObject().get("year").getAsInt()));
		    table.put(put);
		});
		connection.getTable(TableName.valueOf("words"), (table) -> {
                    for (JsonElement
			 element : body.getAsJsonObject()
			               .get("words")
			               .getAsJsonArray()) {
			Put
		        put = new Put(Bytes.toBytes(element.getAsJsonObject().get("word").getAsString()));
			put.addColumn(Bytes.toBytes("frequency"),
				      Bytes.toBytes(documentId),
				      Bytes.toBytes(element.getAsJsonObject().get("frequency").getAsInt()));
			table.put(put);
		    }
		});
	    });
            return;
        }
        response.sendError(HttpServletResponse.SC_BAD_REQUEST);
        return;
    }


    JsonElement parseJson(HttpServletRequest request) throws IOException {
        StringBuilder
        buffer = new StringBuilder();
        
        String
        line;
        
        while ((line = request.getReader().readLine()) != null) {
            buffer.append(line);
        }

        return JsonParser.parseString(buffer.toString());
    }

    private void
    sendAsJson(HttpServletResponse
	       response,
	       String
	       string) throws IOException {
	response.setContentType("application/json");
        PrintWriter
        out = response.getWriter();
	out.print(string);
	out.flush();
    }
}
