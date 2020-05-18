package io.github.raefaldhia;

import io.github.raefaldhia.dao.DocumentDAO;
import io.github.raefaldhia.model.Document;

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

import org.bson.types.ObjectId;

@WebServlet("/documents/*")
public class DocumentsController extends HttpServlet {
    private static final long 
    serialVersionUID = 1L;

    /**
     * /models/
     * /models/id
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        Gson gson = new Gson();
        
        String pathInfo = request.getPathInfo();

        // handle /documents/
        if (pathInfo == null || pathInfo.equals("/")) {
            String 
            result = gson.toJson(
                DocumentDAO
                    .GetInstance()
                    .getAll());

            sendAsJson(response, result);

            return;
        }
        
        // handle query ?words=word,+word,word&name=skript
        if (pathInfo.length() > 2 && pathInfo.charAt(1) == '?') {
            String
            words = request.getParameter("words");

            String
            name = request.getParameter("name");
            
            String
            year = request.getParameter("year");

            String
            author = request.getParameter("author");


            // TODO

            return;
        }

        String[] splits = pathInfo.split("/");
		
		if (splits.length != 2) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
        }
        
        String documentId = splits[1];

        Document
        document = DocumentDAO
            .GetInstance()
            .get(new ObjectId(documentId));

        String
        result = gson
            .toJson(document);

        sendAsJson(response, result);

        return;
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        Gson gson = new Gson();
        
        String pathInfo = request.getPathInfo();

        // handle /documents/
        if (pathInfo == null || pathInfo.equals("/")) {
            StringBuilder
            buffer = new StringBuilder();
            
            String
            line;
            
            while ((line = request.getReader().readLine()) != null) {
	            buffer.append(line);
	        }
	    
	        Document document = gson.fromJson(buffer.toString(), Document.class);

            DocumentDAO
                .GetInstance()
                .put(document);

            Map<String, Object> map = new HashMap<>();

            map.put("id", document.getId().toString());

            sendAsJson(response, gson.toJson(map));

            return;
        }

        response.sendError(HttpServletResponse.SC_BAD_REQUEST);

        return;
    }

    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        Gson 
        gson = new Gson();

        String
        pathInfo = request.getPathInfo();

        if (pathInfo == null || pathInfo.equals("/")){
            response.sendError(HttpServletResponse.SC_BAD_REQUEST);

			return;
		}

        String[] 
        splits = pathInfo.split("/");

        // /documents/$id
        if (splits.length == 2) {	

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