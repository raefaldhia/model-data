package io.github.raefaldhia;

import java.io.IOException;
import java.io.PrintWriter;

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

import org.bson.Document;
import org.bson.types.ObjectId;

import io.github.raefaldhia.dao.Database;

@WebServlet("/documents/*")
public class DocumentsController extends HttpServlet {
    private static final long 
    serialVersionUID = 1L;

    @Override
    protected void 
    doGet(HttpServletRequest
          request, 
          HttpServletResponse
          response) throws ServletException,
                           IOException {
        if (request.getPathInfo() == null || 
            request.getPathInfo()
                   .equals("/")) {
            JsonArray
            documents = new JsonArray();
            Database.GetInstance()
                    .getDatabase((db) -> {
                        for (Document
                             bsonDocument : db.getCollection("documents")
                                              .find()) {
                            JsonObject 
                            document = new JsonObject();
                            document.addProperty("author",
                                                 bsonDocument.getString("author"));
                            document.addProperty("name",
                                                 bsonDocument.getString("name"));
                            document.addProperty("year",
                                                 bsonDocument.getInteger("year"));
                            Database.GetInstance()
                                    .getDatabase((db2) -> {
                                        JsonArray
                                        words = new JsonArray();
                                        for (Document bsonWord : db2.getCollection("words")
                                                                    .find(Filters.eq("document", 
                                                                                     bsonDocument.getObjectId("_id")))) {
                                            JsonObject
                                            word = new JsonObject();
                                            word.addProperty("word", 
                                                             bsonWord.getString("word"));
                                            word.addProperty("frequency",
                                                             bsonWord.getInteger("frequency"));                                            
                                            words.add(word);
                                        }
                                        document.add("words", words);
                                    });
                            documents.add(document);
                        }
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
            Database.GetInstance()
                    .getDatabase((db) -> {
                        final ObjectId
                        documentId = new ObjectId();
                        db.getCollection("documents")
                          .insertOne(new Document("_id", documentId).append("author", 
                                                                            body.getAsJsonObject().get("author").getAsString())
                                                                    .append("name", 
                                                                            body.getAsJsonObject().get("name").getAsString())
                                                                    .append("year",
                                                                            body.getAsJsonObject().get("year").getAsInt()));
                        for (JsonElement element : body.getAsJsonObject()
                                                       .get("words").getAsJsonArray()) {
                            db.getCollection("words")
                              .insertOne(new Document().append("word",
                                                               element.getAsJsonObject().get("word").getAsString())
                                                       .append("frequency",
                                                               element.getAsJsonObject().get("frequency").getAsInt())
                                                       .append("document",
                                                               documentId));                            
                        }
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

    private void sendAsJson(HttpServletResponse response, String string) throws IOException {
		response.setContentType("application/json");
        PrintWriter 
        out = response.getWriter();
		out.print(string);
		out.flush();
	}
}