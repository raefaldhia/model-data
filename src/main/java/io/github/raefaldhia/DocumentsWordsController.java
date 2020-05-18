package io.github.raefaldhia;

import io.github.raefaldhia.dao.DocumentDAO;
import io.github.raefaldhia.dao.WordDAO;
import io.github.raefaldhia.model.Document;
import io.github.raefaldhia.model.Word;
import io.github.raefaldhia.model.WordDocument;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.bson.types.ObjectId;

@WebServlet("/documents.words/*")
public class DocumentsWordsController extends HttpServlet {
    private static final long 
    serialVersionUID = 1L;

    /**
     * /documents.words/$documentId
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        Gson gson = new Gson();
        
        // /documents.words/$documentId
        // RESPONSE
        // {
        //     "words": [
        //         {"id": "word-1", "frequency": 20}
        //     ]
        // }
		if (request.getPathInfo().split("/").length == 2) {

        }
        
        response.sendError(HttpServletResponse.SC_BAD_REQUEST);

        return;
    }

    /**
     * /documents.words/$documentId
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        final Gson 
        gson = new Gson();
        
        final String
        pathInfo = request.getPathInfo();

        final String[] 
        splits = pathInfo.split("/");

        // /documents.words/$documentId
        // REQUEST
        // {
        //     "words": [
        //         "word-1",
        //         "word-2",
        //         "word-3"
        //     ]
        // }
        if (pathInfo.split("/").length == 2) {	
            final Map<String, Object>
            body = gson.fromJson (
                request
                    .getReader()
                    .lines()
                    .collect(Collectors.joining()),
                new TypeToken<Map<String, Object>>(){}.getType());

            for (
                Map<String, Object>
                data 
                    : 
                (List<Map<String, Object>>)
                body.get("words")) {
                data.get("frequency");

                Word
                word = WordDAO
                    .GetInstance()
                    .get((String) data.get("id"));

                Set<WordDocument>
                documents = word.getDocuments();

                WordDocument
                document = new WordDocument (
                    new ObjectId(request.getPathInfo().split("/")[1]), 
                    (int) data.get("frequency"));

                if (!documents.add(document)) {
                    documents.remove(document);
                    documents.add(document);
                }

                WordDAO
                    .GetInstance()
                    .put(new Word(word.getWord(), documents));
            }

            return;
       }

        response.sendError(HttpServletResponse.SC_BAD_REQUEST);

        return;
    }

    private void sendAsJson(HttpServletResponse response, String string) throws IOException {
		response.setContentType("application/json");
		
        PrintWriter 
        out = response.getWriter();
		  
		out.print(string);
		out.flush();
	}
}