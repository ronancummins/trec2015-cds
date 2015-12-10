package searching;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.SPUDLMSimilarity;


/**
 *
 * @author ronanc
 */
public class CustomQuery {
    
    private final static Logger logger = Logger.getLogger(CustomQuery.class.getName());  
    
    private TreeMap<String,Double> bag;

    

    private Double mass;
    
    
    public static boolean QuerySmoothing = false;
    
    
    public static int QueryMethod;
    
    public static int QueryJM = 1;
    public static int QueryDir = 2;
    public static int QuerySPUD = 3;
    
    public static double QueryParam;
    
    public CustomQuery(){
        bag = new TreeMap<String,Double>();
        mass = 0.0;

    }
    
    
    public void empty(){
        bag.clear();
        mass = 0.0;
    }
    
    
    public void add(String str, Double f){
        
        Double c = bag.get(str);
        
        
        if (c==null){
            bag.put(str, f);
        }else{
            bag.put(str, c+f);
        }
        mass += f;
    }
    
    
    
    public void remove(String str, Double f){
        Double c = bag.get(str);
        
        if (c==null){
            bag.put(str, -f);
        }else{
            bag.put(str, c-f);
        }
        mass -= f;
    }
    
   
    
    
    public double get(String str){
        
        Double ret = bag.get(str);
        
        if (ret == null){
            return 0;
        }else{
            return ret;
        }
        
    }
    
    //types in bag
    public int numTypes(){
        return this.bag.size();
    }
    
    //raw mass of query bag
    public double mass(){
        return this.mass;
    }
    
    

    
    //
    // returns the string representation
    public String toString(){
        StringBuilder sb = new StringBuilder();
        for (String key: this.bag.keySet()){
            sb.append(key).append(" ");
        }
        return sb.toString();
    }
    
    
    public boolean contains(String term){
        return this.bag.containsKey(term);
    }
    
    public String examine(){
        ArrayList<Map.Entry<String, Double>> l = QueryExpansion.sortValue(bag);
        return "Mass: " + this.mass + " " + l.toString();
    }    
    

    public Set<String> terms(){
        return bag.keySet();
    }
    
    
    
    public void extractQueryTopic(IndexReader reader) throws IOException{
        
        //this is the smoothing parameter for a background Polya
        double pi;
        
        if (CustomQuery.QueryMethod == CustomQuery.QuerySPUD ){
            pi = SPUDLMSimilarity.b0 * CustomQuery.QueryParam / 
                            (bag.keySet().size() * (1 - CustomQuery.QueryParam) + SPUDLMSimilarity.b0 * CustomQuery.QueryParam);
        }else if (CustomQuery.QueryMethod == CustomQuery.QueryJM ){
            pi = CustomQuery.QueryParam;
            //pi = SPUDLMSimilarity.b0 * CustomQuery.QueryParam / 
            //                (1 * (1 - CustomQuery.QueryParam) + SPUDLMSimilarity.b0 * CustomQuery.QueryParam);
        }else if (CustomQuery.QueryMethod == CustomQuery.QueryDir ){
            pi = CustomQuery.QueryParam/(CustomQuery.QueryParam + mass);
        }else{
            pi = SPUDLMSimilarity.b0 * CustomQuery.QueryParam / 
                            (bag.keySet().size() * (1 - CustomQuery.QueryParam) + SPUDLMSimilarity.b0 * CustomQuery.QueryParam);
        }
        
        double pC;
        double pQ, w;
        double topic=0;
        //logger.info(currentQuery.toString() + "\t" + spud_pi );
        for(String term : bag.keySet()){
            
            pQ = bag.get(term)/mass;
            if (CustomQuery.QueryMethod == CustomQuery.QuerySPUD ){
                pC = (double)reader.docFreq(new Term("text",term)) / reader.getSumDocFreq("text");
            }else{
                pC = (double)reader.totalTermFreq(new Term("text",term)) / reader.getSumTotalTermFreq("text");
            }
                   
            
            
            w = bag.get(term) * (1.0-pi)*pQ /((1.0-pi)*pQ + pi*pC);
            
            //w = (1.0-pi)*pQ /((1.0-pi)*pQ + pi*pC);
            
            //logger.info(term + "\t" + w + "\t" + bag.get(term) + "\t" + pi);
            if (w > 0){
                topic += w;
            }
            
            bag.put(term, w);
            
            //logger.info(term + "\t" + reader.numDocs() + "\t" + reader.docFreq(new Term("text",term)) + "\t"+ query_info);
        }
        //logger.info(l2norm + "\t" + mass);
        mass = topic;
        
    }    
    
}
