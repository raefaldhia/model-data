package io.github.raefaldhia;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.github.raefaldhia.dao.Database;
import io.github.raefaldhia.database.HBase.HBaseConnection;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class DocumentsControllerHBaseTest {
    @Mock
    HttpServletRequest request;

    @Mock
    HttpServletResponse response;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Ignore
    @Test
    public void 
    insert() throws ServletException,
                    IOException {
	this.clearTable();
        JsonObject
        document = new JsonObject();
        document.addProperty("author", "Возера Радасці");
        document.addProperty("name", "skripsi-1");
        document.addProperty("year", 2020);

        JsonArray
        words = new JsonArray();
        document.add("words", words);

        JsonObject
        word = new JsonObject();
        word.addProperty("word", "Alpha");
        word.addProperty("frequency", "1");
        words.add(word);
        word = new JsonObject();
        word.addProperty("word", "Beta");
        word.addProperty("frequency", "2");
        words.add(word);
        word = new JsonObject();
        word.addProperty("word", "Charlie");
        word.addProperty("frequency", "4");
        words.add(word);

        final Gson
        gson = new Gson();

        DocumentsControllerHBase
        documentsController = new DocumentsControllerHBase();

        when(request.getPathInfo())
                    .thenReturn("/");

        when(request.getContentType())
                    .thenReturn("application/json");

        when(request.getReader())
                    .thenReturn(new BufferedReader(new StringReader(gson.toJson(document))));

        StringWriter 
        responseStringWriter = new StringWriter();

        when(response.getWriter())
            .thenReturn(
                new PrintWriter(responseStringWriter));

        documentsController
            .doPost(request, response);

        documentsController = new DocumentsControllerHBase();

        when(request.getPathInfo())
                    .thenReturn("/");
        
	responseStringWriter = new StringWriter();

	when(response.getWriter()).thenReturn(new PrintWriter(responseStringWriter));

        documentsController.doGet(request, response);

        JsonArray
        documents = JsonParser.parseString(responseStringWriter.toString())
                              .getAsJsonArray();

        assertEquals("Возера Радасці",
                     documents.get(0)
                              .getAsJsonObject()
                              .get("author")
                              .getAsString());
        assertEquals("skripsi-1",
                     documents.get(0)
                              .getAsJsonObject()
                              .get("name")
                              .getAsString());
        assertEquals(2020,
                     documents.get(0)
                              .getAsJsonObject()
                              .get("year")
                              .getAsInt());
   }

    void  clearTable() throws IOException {
	HBaseConnection.Create((connection) -> {
            if (connection.getAdmin()
		          .isTableAvailable(TableName.valueOf("documents"))) {
		connection.getAdmin()
		          .disableTable(TableName.valueOf("documents"));
		connection.getAdmin()
		          .deleteTable(TableName.valueOf("documents"));
	    }
	    connection.getAdmin()
		      .createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf("documents"))
				                         .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("information".getBytes())
									                               .build())
           			                         .build());
            if (connection.getAdmin()
		          .isTableAvailable(TableName.valueOf("words"))) {
		connection.getAdmin()
		          .disableTable(TableName.valueOf("words"));
		connection.getAdmin()
		          .deleteTable(TableName.valueOf("words"));
	    }
	    connection.getAdmin()
		      .createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf("words"))
				                         .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("frequency".getBytes())
									                               .build())
				                         .build());
        });	    
    }
}