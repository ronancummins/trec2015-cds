package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Comparator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;

/**
 *
 * @author ronanc
 */
public class Utils {

    private final static Logger logger = Logger.getLogger(Utils.class.getName());

    
    private static CharArraySet stoplist = new CharArraySet(1000, true);
    
    public static EnglishAnalyzer newAnalyzer() throws FileNotFoundException, IOException{
 
        String file = "/home/ronanc/Dropbox/Code/lucene/aux/stopwords.english.large";
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                stoplist.add(line);
            }
        }
        
        return new EnglishAnalyzer(stoplist);
        
    }
    
    public static String applyAnalyzer(String text, Analyzer analyzer) throws IOException{
        String term;
        StringBuilder sb = new StringBuilder();
        
        
        //ad in these two lines for extra-stopwords
        
        analyzer = newAnalyzer();
        
        
        
        TokenStream ts = analyzer.tokenStream("myfield", new StringReader(text));
        //OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
        try {
            ts.reset(); // Resets this stream to the beginning. (Required)
            while (ts.incrementToken()) {
                term = ts.getAttribute(CharTermAttribute.class).toString();
                sb.append(term).append(" ");
            }
            ts.end();
        } finally {
            ts.close();
        }         
        
        
        return sb.toString();
    }  
    
    public synchronized static String tidyWord(String str){
       
        if(str.matches("[-]+")){
            return "";
        }else if(str.length() > 20){
            return "";
        }else if( str.startsWith("<") && str.endsWith(">")){
            return "";
        }else {
            
            //to lower case
            str = str.toLowerCase();

            //replace any non word characters
            str = str.replaceAll("[^a-zA-Z0-9- ]", "");

            

            return str;
        }
        
    }    
    
    
    
    public static String strip_whitespace(String word){
        word = word.replaceAll("[^a-zA-Z0-9-]", "");
        return word;
    }
    

   
}
