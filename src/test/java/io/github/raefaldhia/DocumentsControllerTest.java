package io.github.raefaldhia;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.github.raefaldhia.dao.DocumentDAO;
import io.github.raefaldhia.model.Document;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class DocumentsControllerTest {
    @Mock
    HttpServletRequest request;

    @Mock
    HttpServletResponse response;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        DocumentDAO
            .GetInstance()
            .clear();
    }

    @Test
    public void insert() throws ServletException, IOException {
        final DocumentsController 
        documentsController = new DocumentsController();

        final Gson
        gson = new Gson();

        final Map<String, Object>
        map = new HashMap<>();

        map.put("name", "skripsi-1");
        map.put("author", "Возера Радасці");
        map.put("year", "2020");

        when(request.getPathInfo())
            .thenReturn("/");

        when(request.getContentType())
            .thenReturn("application/json");

        when(request.getReader())
            .thenReturn(
                new BufferedReader(
                    new StringReader(gson.toJson(map))));

        final StringWriter 
        responseStringWriter = new StringWriter();

        when(response.getWriter())
            .thenReturn(
                new PrintWriter(responseStringWriter));

        documentsController
            .doPost(request, response);
        
        final Map<String, Object> 
        resultMap = gson.fromJson (
            responseStringWriter.toString(), 
            new TypeToken<Map<String, Object>>(){}.getType());

        final Document
        newDocument = DocumentDAO
            .GetInstance ()
            .get (
                new ObjectId(
                    String.valueOf(
                        resultMap.get("id"))));

        verify(response).setContentType("application/json");
        assertEquals("skripsi-1", newDocument.getName());
        assertEquals("Возера Радасці", newDocument.getAuthor());
        assertEquals("2020", newDocument.getYear());
    }

    @Test
    public void getAll() throws ServletException, IOException {
        DocumentDAO
            .GetInstance()
            .clear();
  
        DocumentDAO
            .GetInstance()
            .put(new Document("Sebuah Skripsi", "2020", "John Doe"));

        DocumentDAO
            .GetInstance()
            .put(new Document("Duabuah Skripsi", "2020", "John Doe"));
    
        final DocumentsController 
        documentsController = new DocumentsController();

        final Gson
        gson = new Gson();

        when(request.getPathInfo()).thenReturn("/");

        final StringWriter 
        responseStringWriter = new StringWriter();

        when(response.getWriter())
            .thenReturn(new PrintWriter(responseStringWriter));

        documentsController.doGet(request, response);
        
        final List<Map<String, Object>>
        resultMap = gson.fromJson (
            responseStringWriter.toString(),
            new TypeToken<List<Map<String, Object>>>(){}.getType());

        verify(response).setContentType("application/json");
        assertEquals(2, resultMap.size());
        assertEquals("Sebuah Skripsi", resultMap.get(0).get("name"));
        assertEquals("John Doe", resultMap.get(0).get("author"));
        assertEquals("2020", resultMap.get(0).get("year"));
    }

    @Test
    public void getOne() throws ServletException, IOException {
        DocumentDAO
            .GetInstance()
            .clear();

        Document
        document = new Document("Sebuah Skripsi", "2020", "John Doe");

        DocumentDAO
            .GetInstance()
            .put(document);

        final DocumentsController 
        documentsController = new DocumentsController();

        final Gson
        gson = new Gson();

        when(request.getPathInfo())
            .thenReturn("/" + document.getId().toString());

        final StringWriter 
        responseStringWriter = new StringWriter();

        when(response.getWriter())
            .thenReturn(new PrintWriter(responseStringWriter));

        documentsController.doGet(request, response);
        
        final Map<String, Object>
        resultMap = gson.fromJson (
            responseStringWriter.toString(),
            new TypeToken<Map<String, Object>>(){}.getType());

        verify(response).setContentType("application/json");
        assertEquals("Sebuah Skripsi", resultMap.get("name"));
        assertEquals("John Doe", resultMap.get("author"));
        assertEquals("2020", resultMap.get("year"));
    }
}