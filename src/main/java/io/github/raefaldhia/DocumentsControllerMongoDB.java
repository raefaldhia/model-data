package io.github.raefaldhia;

import java.io.IOException;
import java.io.PrintWriter;
import java.time.Year;
import java.util.Arrays;
import java.util.Optional;

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

import io.github.raefaldhia.model.Document;
import io.github.raefaldhia.model.Document.Word;
import io.github.raefaldhia.repository.MongoDBDocumentRepository;


@WebServlet("/documents-mongodb/*")
public class DocumentsControllerMongoDB extends HttpServlet {
    private static final long
    serialVersionUID = 1L;

    private final Gson
    gson;

    public DocumentsControllerMongoDB() {
        this.gson = new Gson();
    }

    @Override
    protected void 
    doDelete(final HttpServletRequest request,
             final HttpServletResponse response) throws ServletException, IOException {
        MongoDBDocumentRepository.delete(request.getParameter("id"));
    }

    @Override
    protected void
    doGet(final HttpServletRequest request,
          final HttpServletResponse response) throws ServletException, IOException {
        if (request.getPathInfo() == null || request.getPathInfo().equals("/")) {
            final JsonArray
            documents = new JsonArray();

            MongoDBDocumentRepository.filter(Optional.ofNullable(request.getParameter("author"))
                                                     .orElse(null),
                                             Optional.ofNullable(request.getParameter("name"))
                                                     .orElse(null),
                                             Optional.ofNullable(request.getParameter("year"))
                                                     .map(value -> Year.of(Integer.parseInt(value)))
                                                     .orElse(null),
                                             Optional.ofNullable(request.getParameter("words"))
                                                     .map(value -> Arrays.asList(value.split(" ")))
                                                     .orElse(null))
                                     .forEach(document -> { 
                                         JsonObject
                                         obj = this.gson.toJsonTree(document).getAsJsonObject();
                                         obj.addProperty("id", document.getId().toString());               
                                         obj.addProperty("year", document.getYear().getValue());

                                         documents.add(obj);
                                     });

            sendJson(response, documents);

            return;
        }

        response.sendError(HttpServletResponse.SC_BAD_REQUEST);

        return;
    }

    @Override
    protected void
    doPost(final HttpServletRequest request, 
           final HttpServletResponse response) throws ServletException, IOException {
        if ((request.getPathInfo() == null || request.getPathInfo().equals("/")) && request.getContentType().equals("application/json")) {
            final JsonElement
            body = parseJson(request);

            final Document
            document = new Document();
            Optional.ofNullable(body.getAsJsonObject()
                                    .get("author"))
                    .map(author -> author.getAsString())
                    .ifPresent(author -> document.setAuthor(author));
            Optional.ofNullable(body.getAsJsonObject()
                                    .get("name"))
                    .map(name -> name.getAsString())
                    .ifPresent(name -> document.setName(name));
            Optional.ofNullable(body.getAsJsonObject()
                                    .get("year"))
                    .map(year -> year.getAsInt())
                    .ifPresent(year -> document.setYear(Year.of(year)));

            body.getAsJsonObject()
                .get("words")
                .getAsJsonArray()
                .forEach(element -> {
                    final Word
                    word = new Word();
                    Optional.ofNullable(element.getAsJsonObject()
                                               .get("name"))
                            .map(name -> name.getAsString())
                            .ifPresent(name -> word.setName(name));
                    Optional.ofNullable(element.getAsJsonObject()
                                               .get("frequency"))
                            .map(frequency -> frequency.getAsInt())
                            .ifPresent(frequency -> word.setFrequency(frequency));

                    document.getWords()
                            .add(word);
                });

            MongoDBDocumentRepository.save(document);

            return;
        }

        response.sendError(HttpServletResponse.SC_BAD_REQUEST);

        return;
    }

    private JsonElement 
    parseJson(final HttpServletRequest request) throws IOException {
        final StringBuilder
        buffer = new StringBuilder();

        String
        line;

        while ((line = request.getReader().readLine()) != null) {
            buffer.append(line);
        }

        return JsonParser.parseString(buffer.toString());
    }

    private void
    sendJson(final HttpServletResponse response,
             final JsonElement json) throws IOException {
        response.setContentType("application/json");
        
        final PrintWriter
        out = response.getWriter();
        out.print(gson.toJson(json));
        out.flush();
    }
}