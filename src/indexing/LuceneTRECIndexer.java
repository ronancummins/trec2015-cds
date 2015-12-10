
package indexing;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import logging.LoggerInitializer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.*;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Node;
import org.jsoup.safety.Cleaner;
import org.jsoup.safety.Whitelist;
import utils.UncompressInputStream;
import utils.Utils;

/**
 *
 * @author ronanc
 */
public class LuceneTRECIndexer {

    private final static Logger logger = Logger.getLogger( LuceneTRECIndexer.class.getName());
    
    
    public static int zipped = 1;
    public static int doc_types;
    
    private final IndexWriter writer;
    
    private final Analyzer analyzer;
    private int docs=0;
    
    public LuceneTRECIndexer(String location) throws IOException{

        Path p = Paths.get(location);
        analyzer = new EnglishAnalyzer();
        Directory dir = new NIOFSDirectory(p);
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        writer = new IndexWriter(dir, config);
        
    }
    
    
    
    /**
     * We calculate the TotalTerms and UniqueTerms of a document
     * before indexing and store these stats in the index as stored fields
     * (as well as in the index for use during ranking).
     * 
     * This could be optimsed further. 
     * 
     * @param text
     * @param doc_id
     * @throws IOException 
     */
    
    private void addDoc(String text, String doc_id) throws IOException {
        Document doc = new Document();
        HashMap<String, Integer> map = new HashMap();
        int length = 0;
        Integer freq;
        TokenStream ts = analyzer.tokenStream("myfield", new StringReader(text));
        //OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
        try {
            ts.reset(); // Resets this stream to the beginning. (Required)
            while (ts.incrementToken()) {
                //index terms greater than length 1
                if (ts.getAttribute(CharTermAttribute.class).toString().length() > 1){
                    freq = map.get(ts.getAttribute(CharTermAttribute.class).toString());
                    if (freq == null){
                        freq = 0;
                    }
                    freq++;
                    map.put(ts.getAttribute(CharTermAttribute.class).toString(), freq);
                    
                    //logger.info(ts.getAttribute(CharTermAttribute.class) + " " + freq);
                    //logger.info(text.substring(offsetAtt.startOffset(), offsetAtt.endOffset()));
                    length++;
                }
            }
            ts.end();   // Perform end-of-stream operations, e.g. set the final offset.
        } finally {
            ts.close(); // Release resources associated with this stream.
        }        
        
        double entropy=0,p;
        for (String term: map.keySet()){
            p = ((double)map.get(term)/(double)length);
            entropy -= p * Math.log(p);
        }
        
        Long entropy_power = new Double(Math.exp(entropy)).longValue();
        
        
        //index options 
        FieldType vec_field = new FieldType();
        vec_field.setIndexOptions(vec_field.indexOptions().DOCS_AND_FREQS_AND_POSITIONS);
        vec_field.setStored(true);
        vec_field.setStoreTermVectors(false);
        vec_field.setOmitNorms(false);
        
        //important to store document length heres (both UniqueTerms and TotalTerms)
        doc.add(new Field("text", text, vec_field));
        doc.add(new StringField("doc_id", doc_id, Field.Store.YES));
        doc.add(new NumericDocValuesField("UniqueTerms", map.size()));
        doc.add(new NumericDocValuesField("TotalTerms", length));
        doc.add(new NumericDocValuesField("Entropy", entropy_power));
        doc.add(new StoredField("UniqueTerms", map.size()));
        doc.add(new StoredField("TotalTerms", length));
        doc.add(new StoredField("Entropy", entropy_power));        
        
    
        
        //logger.info(length + "\t" + map.size());
        writer.addDocument(doc);
        
        
        //logger.info(doc_id +"\n" + text);
        docs++;
        if ((docs %10000)==0){
            logger.info(docs + " documents indexed ...");
        }
        
    }

    
    
    private void indexTrec(String files) throws FileNotFoundException, IOException{
        
        BufferedReader br = new BufferedReader(new FileReader(files));
        String line;
        while ((line = br.readLine()) != null) {
            
            try{
                if (LuceneTRECIndexer.doc_types == 1){
                    parseNewsFile(line);
                }else if (LuceneTRECIndexer.doc_types == 2){
                    parseTRECWebFile(line);
                }else if (LuceneTRECIndexer.doc_types == 3){
                    parseTRECXMLFile(line);
                }
                
            }catch(Exception e){
                logger.info("Could not index file " + line + ". Message is " + e.toString());
                e.printStackTrace();
            }
            
        }
        
    }
    
    
    /*
    * Quick function to index TREC News format
    *
    */
    
    private void parseNewsFile(String filename) throws IOException{
        
        
        
        BufferedReader br;
        
        if (LuceneTRECIndexer.zipped == 1) {
            //logger.log(Level.INFO, "Zipped");
            InputStream fileStream = new FileInputStream(filename);
            //InputStream gzipStream = new GZIPInputStream(fileStream);
            InputStream gzipStream =new UncompressInputStream(new FileInputStream(filename));
            //Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
            Reader decoder = new InputStreamReader(gzipStream);
            br = new BufferedReader(decoder);
        } else {
            br = new BufferedReader(new FileReader(filename));
        }
        
        String[] terms;
        String doc_id =null;
        boolean index=false;
        StringBuilder doc = null;
        
        String line;
        while ((line = br.readLine()) != null) {
            
            
            
            if (line.startsWith("<!--")){
                continue;            
            }
            
            if (line.startsWith("</DOC>")){
                index = false;
                addDoc(doc.toString(), doc_id);
                doc=null;
            }
            
            if (line.contains("<DOC>")){
                doc = new StringBuilder();
            }
        
            
            if (line.contains("<DOCNO>")){
                StringBuilder sb = new StringBuilder();
                sb.append(line);
                
                if (!line.contains("</DOCNO>")){
                    
                    while (!(line = br.readLine()).contains("</DOCNO>")){
                        sb.append(line);
                    }
                    sb.append(line);
                    index = true;
                }
                //logger.log(Level.INFO, sb.toString());
                doc_id = sb.toString().substring(7, sb.lastIndexOf("</DOCNO>"));
                doc_id = Utils.strip_whitespace(doc_id);
                //local_index.indexTerm(doc_id, " ");
                //logger.log(Level.INFO, doc_id);
                
                
            }
             
            terms = line.split(" ");
            //logger.log(Level.INFO, line);
            String stem;
            
            for (String word:terms){
                
                
                if (index){
                    //logger.log(Level.INFO, "Before|" + doc_id + "|" + word + "|");
                    doc_id = Utils.strip_whitespace(doc_id);
                    //logger.log(Level.INFO, "Indexing" + doc_id + "\t"+ word);
                    if (!((word.contains("<") && word.contains("<")))){
                        doc.append(word).append(" ");
                    }
                }else{
                    //logger.log(Level.INFO, "Not indexing" + doc_id + "\t"+ word);
                }
                                
            }
            
            
            /**
             * turn on for next term
             */
            if ((line.contains("<HEADLINE>"))||(line.contains("<TEXT>"))){
                index = true;
            }            
            
        }
        
        

        br.close();
        
    }

    
    /**
     * Index a TREC format HTML page
     * 
     * @param filename
     * @throws Exception 
     */
    private synchronized void parseTRECWebFile(String filename) throws Exception{
        
        
        //logger.info(" Indexing " + filename);
        
        
        BufferedReader br;
        StringBuilder html = null;
        
        if (LuceneTRECIndexer.zipped == 1) {
            //logger.log(Level.INFO, "Zipped");
            InputStream fileStream = new FileInputStream(filename);
            InputStream gzipStream = new GZIPInputStream(fileStream);
            //InputStream gzipStream =new UncompressInputStream(new FileInputStream(filename));
            Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
            //Reader decoder = new InputStreamReader(gzipStream);
            br = new BufferedReader(decoder);
        } else {
            //logger.log(Level.INFO, "Not Zipped");
            br = new BufferedReader(new FileReader(filename));
        }        
        
        String line;
        String[] terms;
        String doc_id =null;
        boolean index=false;
        
        while ((line = br.readLine()) != null) {
            
            /**
             * turn off for next term
             */
            if (line.startsWith("</DOC>")){
                //logger.log(Level.INFO, convert(html.toString()));
                
                if (html != null) {   
                    
                    String d = convert(html.toString());
                    //logger.info(d);
                    addDoc(d, doc_id);
                }
                index = false;
            }             
 
        
            StringBuilder sb = new StringBuilder();
            if (line.startsWith("<DOCNO>")){
                
                sb.append(line);
                
                if (!line.contains("</DOCNO>")){
                    
                    while (!(line = br.readLine()).contains("</DOCNO>")){
                        sb.append(line);
                    }
                    sb.append(line);
                }
                //logger.log(Level.INFO, sb.toString());
                doc_id = sb.toString().substring(7, sb.lastIndexOf("</DOCNO>"));
                doc_id = Utils.strip_whitespace(doc_id);
                
                
            }
            
            terms = line.split(" ");
            //logger.log(Level.INFO, line);
            String stem;
            
            for (String word:terms){
                
                if (index){

                    html.append(word).append(" ");
                    //logger.log(Level.INFO, "Indexing" + doc_id + "\t"+ word);
                }else{
                    //logger.log(Level.INFO, "Not indexing" + doc_id + "\t"+ word);
                }
                                
            } 
            
            
            /**
             * turn on for next term
             */
            if (line.contains("</DOCHDR>")){ 
                index = true;
                html = new StringBuilder();
            }            
            
            
            
        }
        
        br.close();
        
    }
        
    
    
    /**
     * Index a TREC format HTML page
     * 
     * @param filename
     * @throws Exception 
     */
    private synchronized void parseTRECXMLFile(String filename) throws Exception{
        
        
        //logger.info(" Indexing " + filename);
        
        
        BufferedReader br;
        
        
        if (LuceneTRECIndexer.zipped == 1) {
            //logger.log(Level.INFO, "Zipped");
            InputStream fileStream = new FileInputStream(filename);
            InputStream gzipStream = new GZIPInputStream(fileStream);
            //InputStream gzipStream =new UncompressInputStream(new FileInputStream(filename));
            Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
            //Reader decoder = new InputStreamReader(gzipStream);
            br = new BufferedReader(decoder);
        } else {
            //logger.log(Level.INFO, "Not Zipped");
            br = new BufferedReader(new FileReader(filename));
        }        
        
        String line;
        String[] terms;
        String doc_id =null;
        String text;
        
        String start_delim = "<article-id pub-id-type=\"pmc\">";
        String end_delim = "</article-id>";
        StringBuilder sb = new StringBuilder();
        while ((line = br.readLine()) != null) {

            sb.append(line);

            /**
             * get the id
             */
            if (line.contains(start_delim)) {
                int start = line.indexOf(start_delim) + start_delim.length() ;
                int end = line.indexOf(end_delim, start);
                doc_id = line.substring(start, end);

                //logger.info("id " + doc_id);
            }
            
            
            
            

        }

        
        text = convert(sb.toString().replace("><", "> <"));
            
        //logger.info(doc_id + "\n" + text);
          
        addDoc(text, doc_id);    
            
         
       
        
        br.close();
        
    }
    
    
    /**
     * uses jJSOUP
     * @param html
     * @return 
     */
    private synchronized String convert(String html) {

        org.jsoup.nodes.Document doc = Jsoup.parse(html);
        //Document doc = Jsoup.parse(html);
        removeComments(doc);
        doc = new Cleaner(Whitelist.relaxed()).clean(doc);
        
        String str = doc.text();

        str = str.replaceAll("/", " ");
        str = str.replaceAll("\n", " ");

        return str;
    }

    private synchronized static void removeComments(Node node) {
        for (int i = 0; i < node.childNodes().size();) {
            Node child = node.childNode(i);
            if (child.nodeName().equals("#comment")) {
                child.remove();
            } else {
                removeComments(child);
                i++;
            }
        }
    }   
    
  
    
    
    
    public void close() throws IOException {
        this.writer.close();
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {

        LoggerInitializer.setup();

        if (args.length > 3) {

            String trec_files = args[1];

            LuceneTRECIndexer.doc_types = Integer.parseInt(args[2]);
            LuceneTRECIndexer.zipped = Integer.parseInt(args[3]);
            LuceneTRECIndexer indexer = new LuceneTRECIndexer(args[0]);
            indexer.indexTrec(trec_files);
            

            indexer.close();
        }else{
            logger.info("LuceneTRECIndexer (directory) (indexfile) (1=News, 2=Web, 3=XML) (zipped=1, else 0) \n \n "
                    + "\t\"directory\" will be where the lucene index is created,\n"
                    + "\t\"indexfile\" is a file listing the full paths of the trec files to be indexed,\n"
                    + "\t\"1=News, 2=Web, 3=XML\" determines whether the trec files are in html format or not,\n"
                    + "\t\"zipped=1, else 0\" for zipped or plain-text."
                    + "\n\n\n\tNote: This indexer indexes two numeric fields into each document "
                    + "for document normalisation used during ranking. The XML indexing setting is used "
                    + "for indexing the CDS PubMed collection. \n");
        }
    }

    
    
}
