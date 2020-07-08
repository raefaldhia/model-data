package io.github.raefaldhia.data;

import io.github.raefaldhia.model.Document;
import io.github.raefaldhia.model.Document.Word;
import io.github.raefaldhia.repository.HBaseDocumentRepository;
import io.github.raefaldhia.repository.MongoDBDocumentRepository;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Year;
import java.util.Scanner;

/**
 *
 * @author Evan
 */
public class DataGenerator {
    
    public void generateData() throws FileNotFoundException, IOException{
        File file = new File(System.getProperty("user.dir").concat("\\Data.csv"));
        FileReader fileReader = new FileReader(file);
        BufferedReader csvReader = new BufferedReader(fileReader);
        
        //read first line (header)
        csvReader.readLine();
        
        String row;
        while((row = csvReader.readLine()) != null){
            String[] data = row.split(",");
            Document document = new Document();
            
            //Add Attributes
            document.setName(data[0]);
            document.setYear(Year.parse(data[1]));
            document.setAuthor(data[2]);
            document.getWords().add(new Word(data[3], Integer.parseInt(data[4])));
            document.getWords().add(new Word(data[5], Integer.parseInt(data[6])));
            document.getWords().add(new Word(data[7], Integer.parseInt(data[8])));
            
            HBaseDocumentRepository.save(document);
            MongoDBDocumentRepository.save(document);
        }
        
    }
}
