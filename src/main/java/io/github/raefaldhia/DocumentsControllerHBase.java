package io.github.raefaldhia;

import java.io.IOException;
import java.io.PrintWriter;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.bson.types.ObjectId;

import io.github.raefaldhia.database.HBase.HBaseConnection;


@WebServlet("/documents-hbase/*")
public class DocumentsControllerHBase extends HttpServlet {
    private static final long
    serialVersionUID = 1L;

    private static final Configuration 
    config = HBaseConfiguration.create();

    HashMap<String, Integer>
    getDocumentsByMetadata (final HttpServletRequest
                            request,
                            HttpServletResponse
                            response) throws IOException {
	final Connection
        connection = ConnectionFactory.createConnection(config);
	final Table
	table = connection.getTable(TableName.valueOf("documents"));
	final Scan
        scan = new Scan();
	scan.addFamily("information".getBytes());
	final FilterList
	filter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
	if (request.getParameter("author") != null) {
	    filter.addFilter(new SingleColumnValueFilter("information".getBytes(),
							 "author".getBytes(),
							 CompareOperator.EQUAL,
							 Bytes.toBytes(request.getParameter("author"))));
	}
	if (request.getParameter("name") != null) {
	    filter.addFilter(new SingleColumnValueFilter("information".getBytes(),
							 "name".getBytes(),
							 CompareOperator.EQUAL,
							 Bytes.toBytes(request.getParameter("name"))));
	}
	if (request.getParameter("year") != null) {
	    filter.addFilter(new SingleColumnValueFilter("information".getBytes(),
							 "year".getBytes(),
							 CompareOperator.EQUAL,
							 Bytes.toBytes(Integer.parseInt(request.getParameter("year")))));
	}
	scan.setFilter(filter);
	final ResultScanner
        scanner = table.getScanner(scan);
	final HashMap<String, Integer>
        documents = new HashMap<String, Integer>();
	for (Result
             result = scanner.next();
	     result != null;
	     result = scanner.next()) {
	    documents.put(Bytes.toString(result.getRow()), 0);
	}
	scanner.close();
	table.close();
	connection.close();
	return documents;
    }

    HashMap<String, Integer>
    getDocumentsByWords (final HttpServletRequest
                         request,
                         HttpServletResponse
                         response) throws IOException {
	final Connection
        connection = ConnectionFactory.createConnection(config);
	final Table
	table = connection.getTable(TableName.valueOf("words"));
	final Scan
        scan = new Scan();
	final FilterList
	filter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
	if (request.getParameter("words") != null) {
	    for (String
		     word : request.getParameter("words")
		                   .split(" ")) {
		filter.addFilter(new RowFilter(CompareOperator.EQUAL,
					       new BinaryComparator(word.getBytes())));
	    }
	}
	scan.setFilter(filter);
	final ResultScanner
	scanner = table.getScanner(scan);
	final HashMap<String, Integer>
        documents = new HashMap<String, Integer>();
	if (filter.size() != 0) {
	    for (Result
                 wordResult = scanner.next();
		 wordResult != null;
		 wordResult = scanner.next()) {
		Map<byte[], byte[]>
		frequencies = wordResult.getFamilyMap("frequency".getBytes());
		for (byte[]
		    keyBytes : frequencies.keySet()) {
		    String
			key = Bytes.toString(keyBytes);
		    Integer
			value = documents.get(key);
		    if (value == null) {
			value = 0;
		    }
		    documents.put(key, value + 1);
		}
	    }
	}
	scanner.close();
	table.close();
	connection.close();
	return documents;
    }

    @Override
    protected void 
    doGet(final HttpServletRequest
          request,
          HttpServletResponse
          response) throws ServletException,
                           IOException {
        if (request.getPathInfo() == null || 
            request.getPathInfo()
                   .equals("/")) {
	    HashMap<String, Integer>
	    metadataDocumentMap = this.getDocumentsByMetadata(request, response);
	    HashMap<String, Integer>
	    wordsDocumentMap = this.getDocumentsByWords(request, response);
	    JsonArray
            documents = new JsonArray();
	    for(String
		documentId : metadataDocumentMap.keySet()) {
		if (wordsDocumentMap.get(documentId) == null) {
		    continue;
		}
		JsonObject
		document = new JsonObject();
		document.addProperty("id",
				     documentId);
		documents.add(document);		
	    }
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
