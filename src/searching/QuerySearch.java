package searching;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import logging.LoggerInitializer;
import org.apache.commons.math3.special.Gamma;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.SPUDLMSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import searching.Evaluator.RankedList;
import utils.Utils;

/**
 *
 * @author ronanc
 */
public class QuerySearch {
    
    private final static Logger logger = Logger.getLogger( QuerySearch.class.getName());
 
    private final IndexSearcher searcher;
    private final IndexReader reader;
    private final Analyzer analyzer;
    
    //for reading actual documents for expansion
    private IndexSearcher documents_searcher;
    private IndexReader documents_reader;
        
    public static int stemmed = 1;
     
    public static final int type = 1;
    public static final int type_summ = 2;
    public static final int type_desc_summ = 3;
    public static final int desc = 4;
    public static final int summ = 5;

    private final HashMap<String, String[]> type_queries;
    private final HashMap<String, String[]> desc_queries;
    private final HashMap<String, String[]> summ_queries;
    private final HashMap<String, String[]> type_summ_queries;
    private final HashMap<String, String[]> type_desc_summ_queries;
    
    private final TreeMap<String,Double> AP_values;
    private final TreeMap<String,Double> PREC_values;
    private final TreeMap<String,Double> NDCG10_values;
    private final TreeMap<String,Double> NDCG20_values;
    
    
    public static int query_type = 1;
    
    private Evaluator eval;
    
    private HashMap<String, String[]> current_set;
    
    private int max_iterations = 10;
    
    public static CustomQuery currentQuery;
    
    public static Set<String> loadfields;
    
    public QuerySearch(String location) throws IOException{

       
        Path p = Paths.get(location);
        
        Path p2 = Paths.get(location + "_documents");
        
        Directory dir = new NIOFSDirectory(p);
        Directory documents_dir = new NIOFSDirectory(p2);
        
        reader = DirectoryReader.open(dir);
        searcher = new IndexSearcher(reader);
        
        documents_reader = DirectoryReader.open(documents_dir);
        documents_searcher = new IndexSearcher(documents_reader);
        

        
        if (QuerySearch.stemmed == 1){
            logger.info("English Analyzer with stemming");
            analyzer = new EnglishAnalyzer();
        }else{
            logger.info("Standard Analyzer without stemming");
            analyzer = new StandardAnalyzer();
        }
        
        type_queries = new HashMap();
        desc_queries = new HashMap();
        summ_queries = new HashMap();        
        type_summ_queries = new HashMap();
        type_desc_summ_queries = new HashMap();
        
        AP_values = new TreeMap<>();
        PREC_values = new TreeMap<>();
        NDCG10_values = new TreeMap<>();
        NDCG20_values = new TreeMap<>();
               
        currentQuery = new CustomQuery();
        
        loadfields = new TreeSet();
        loadfields.add("UniqueTerms");
        loadfields.add("TotalTerms");
        loadfields.add("doc_id");     
    }
    
    private void clearMaps(){
        this.AP_values.clear();
        this.NDCG10_values.clear();
        this.NDCG20_values.clear();
    }
    /**
     * 
     * This is called if SPUDLMSimilarity.b0Set is false
     * Estimates the background DCM mass
     * @return
     * @throws IOException 
     */
    
    public double estimateB0() throws IOException {

        
        // now estimate mass of background model (DCM)
        //
        logger.info("estimate background DCM mass...");
        double denom;
        double s = 250;
        for (int i = 0; i < max_iterations; i++) {
            logger.log(Level.INFO, "iteration " + i + " estimated mu value is " + s );
            denom = 0;
            
            for (int j = 0; j < reader.maxDoc(); j++) {
                
                Document doc = reader.document(j);
                
                String[] str_dl =  doc.getValues("TotalTerms");
                //logger.info(str_dl[0] + "");
                Double dl = Double.parseDouble(str_dl[0]);
                denom += Gamma.digamma(s + dl);
            }
            
            
            denom =  (denom - (reader.getDocCount("text") * Gamma.digamma(s)));
            
            s = reader.getSumDocFreq("text")/denom;

        }        
        logger.info("done.");
        
        
        
        return s;
    }
    
    
    
    /**
     * prels is not implemented here. 
     * Just use qrels
     * @param _qrels
     * @throws IOException 
     */
    public void loadQrels(String _qrels) throws IOException{
        eval = new Evaluator(_qrels);
    }
    
    
    
    
    
    public String[] clean_line(String q){
        
        StringBuilder sb = new StringBuilder();
        
        for (String w:q.split(" ")){
            sb.append(Utils.tidyWord(w)).append(" ");
            
        }
        
        return sb.toString().split(" ");
    }
    
    
    /**
     * 
     * This just loads queries, for iterating through later
     * 
     * @param fname
     * @throws FileNotFoundException
     * @throws IOException 
     */
    
    
    public void loadXMLTopics(String fname) throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new FileReader(fname));
        String line = "";
        String type = "";
        String desc = "";
        String summary = "";
        String diagnosis = "";
        String num = "";
        int s, e;
        
        while ((line = br.readLine()) != null) {
            
            if (line.contains("<topic number=")) {
                s = line.indexOf("=") + 2;
                e = line.indexOf("\"", s);
                num = line.substring(s, e);

                s = line.indexOf("type") + 6;
                e = line.indexOf("\">", s);
                type = line.substring(s, e);
                
            }

            if (line.contains("<description>")) {
                s = line.indexOf("<description>") + 13;
                e = line.indexOf("</description>", s);
                desc = line.substring(s, e);
                desc = desc.replace("[^.,()]", " ").toLowerCase();
            }

            if (line.contains("<summary>")) {
                s = line.indexOf("<summary>") + 9;
                e = line.indexOf("</summary>", s);
                summary = line.substring(s, e);
                
                //logger.info(num + "\t|\t" + Arrays.toString(clean_line(type)) + "\t|\t" + Arrays.toString(clean_line(desc)) 
                //        + "\t|\t" + Arrays.toString(clean_line(summary)));
                
            }
            
            
            if (line.contains("<diagnosis>")) {
                s = line.indexOf("<diagnosis>") + 11;
                e = line.indexOf("</diagnosis>", s);
                diagnosis = line.substring(s, e);
                
                //logger.info(num + "\t|\t" + Arrays.toString(clean_line(type)) + "\t|\t" + Arrays.toString(clean_line(desc)) 
                //        + "\t|\t" + Arrays.toString(clean_line(summary)));
                //logger.info(diagnosis + " ");
                
            }            
            
            if (line.contains("</topic>")){
                type_queries.put(num, type.split(" "));
                type_summ_queries.put(num, clean_line(type + " " + summary));
                desc_queries.put(num, clean_line(desc+ " " + diagnosis));
                summ_queries.put(num, clean_line(summary + " " + diagnosis));
                type_desc_summ_queries.put(num, clean_line(type + " " + summary + " " + desc));
            }
            
            
        }
        
        logger.log(Level.INFO, type_queries.size() + " title queries loaded ... ");
        logger.log(Level.INFO, desc_queries.size() + " desc queries loaded ... ");
        logger.log(Level.INFO, summ_queries.size() + " summ queries loaded ... ");
        br.close();        
        
        
    }
  
    
    public void setQuerySet(int type) {

        QuerySearch.query_type = type;
        if (QuerySearch.query_type == QuerySearch.type) {
            this.current_set = type_queries;
        } else if (QuerySearch.query_type == QuerySearch.type_summ) {
            this.current_set = type_summ_queries;
        } else if (QuerySearch.query_type == QuerySearch.type_desc_summ) {
            this.current_set = type_desc_summ_queries;
        } else if (QuerySearch.query_type == QuerySearch.desc) {
            this.current_set = desc_queries;
        } else if (QuerySearch.query_type == QuerySearch.summ) {
            this.current_set = summ_queries;
        } else {
            this.current_set = type_queries;
        }

        int avg_len = 0;
        for (String key : current_set.keySet()) {

            String[] val = current_set.get(key);
            avg_len += val.length;

        }

        logger.info("Average query length: " + (double) avg_len / (double) current_set.size());
    }
    
    
    /**
     * Get the mean of the results
     * @param map
     * @return 
     */
    public double Mean(TreeMap<String,Double> map) {
        double mean = 0.0;

        
        for(String key: map.keySet()){
            mean += (Double)map.get(key);
        }
        mean = mean / map.size();

        return mean;
    }    
    
    
    
    /**
     * Run a query 
     * @param key
     * @throws ParseException
     * @throws IOException 
     */
    
    /**
     * Run a query 
     * @param key
     * @throws ParseException
     * @throws IOException 
     */
    
    public void runQuery(String key) throws ParseException, IOException {
        
        currentQuery.empty();
        
        //check that the query is not empty
        String[] query_array = current_set.get(key);
        if (query_array.length == 1){
            if (query_array[0].trim().equals("")){
                return;
            }
        }
        
        
        //ok
        
        StringBuilder query_str = new StringBuilder();
        for (String s : query_array) {
            
            query_str.append(s).append(" ");
            
        }
        
        //logger.info(query_str.toString());
        
        //custom representation of query (stemmed)
        String prep_query = Utils.applyAnalyzer(query_str.toString(), analyzer);
        for (String s: prep_query.split(" ")){
            currentQuery.add(s, 1.0);
        }         
        
        
        if (CustomQuery.QuerySmoothing){
            currentQuery.extractQueryTopic(reader);
        }
        //logger.info("Query: " + currentQuery.examine());
        
        
        Query query = new QueryParser("text", new StandardAnalyzer()).parse(currentQuery.toString());
        
        //run the SPUD dir method
        Query lmnorm_query = new LMNormQuery(query);
        searcher.setSimilarity(new SPUDLMSimilarity());
        TopDocs ret = searcher.search(lmnorm_query, 1000);
        
 
        

        //could do multiple rounds of relevance expansion if (r > 1)
        for (int r = 0; r < 1; r++) {
            if (QueryExpansion.EXPAND) {
                RankedList[] orig_ranking = new RankedList[ret.scoreDocs.length];
                QueryExpansion expander = new QueryExpansion();

                for (int i = 0; i < ret.scoreDocs.length; i++) {

                    orig_ranking[i] = new RankedList();
                    orig_ranking[i].doc_id = searcher.doc(ret.scoreDocs[i].doc).get("doc_id");
                    orig_ranking[i].score = ret.scoreDocs[i].score;

                    //assume top docs are relevant and add to models
                    if (i < QueryExpansion.pseudo_rel_docs) {
                        //logger.info(orig_ranking[i].doc_id);
                        Query doc_lookup = new TermQuery(new Term("doc_id", orig_ranking[i].doc_id));
                        
                        //logger.info(q1 + "");
                        TopDocs prels = documents_searcher.search(doc_lookup,1);
                        if (prels.totalHits > 0) {
                            String orig_text = documents_searcher.doc(prels.scoreDocs[0].doc).get("text");

                            if ((QueryExpansion.method == QueryExpansion.PRM1)
                                    || (QueryExpansion.method == QueryExpansion.PRM2)) {
                                expander.addPositionalExpansionDoc(currentQuery, orig_text, orig_ranking[i].score, this.analyzer, reader);
                            } else {
                                expander.addExpansionDoc(orig_text, orig_ranking[i].score, this.analyzer, reader);
                            }
                        } else {
                            logger.warning("Expansion:: Document lookup retrieved no item in document index");
                        }
                    }

                }

                Map<String,Double> e_terms = expander.expansionTerms(this.reader, currentQuery, query_array.length);
                //logger.info(currentQuery.examine());
                
                expander.expandCustomQuery(this.reader, currentQuery, e_terms);
                //logger.info(currentQuery.examine());
                
                query = new QueryParser("text", new StandardAnalyzer()).parse(currentQuery.toString());
                lmnorm_query = new LMNormQuery(query);
                searcher.setSimilarity(new SPUDLMSimilarity());
                ret = searcher.search(lmnorm_query, 1000);
                
                
                
            }
        }
        
        
        
        
        
        // final ranking 
        // 
        
        RankedList[] ranking = new RankedList[ret.scoreDocs.length];
        
        for (int i=0;i<ret.scoreDocs.length;i++){
            ranking[i] = new RankedList();
            ranking[i].doc_id = searcher.doc(ret.scoreDocs[i].doc, QuerySearch.loadfields).get("doc_id").toString();
            ranking[i].score = ret.scoreDocs[i].score;

            //for trec-eval output
            System.out.println(key + "\t" + "Q0" + "\t"
                    + ranking[i].doc_id + "\t" + (i + 1)
                    + "\t" + ranking[i].score + "\t" + "CAM_SPUDr8");

        }
        
        double ap = eval.AP(key, ranking);
        double ndcg10 = eval.NDCG10(key, ranking);
        double ndcg20 = eval.NDCG20(key, ranking);
        double prec10 = eval.prec(key, ranking, 10);
        
        //logger.info(ap + "\t" + ndcg10 + "\t" + ndcg20);
        if (!Double.isNaN(ap)){
            AP_values.put(key, ap);
            NDCG10_values.put(key, ndcg10);
            NDCG20_values.put(key, ndcg20);
            this.PREC_values.put(key, prec10);
            //System.out.println(ap + "\t" + prec10 + "\t" + ndcg10 + "\t" + ndcg20);
        }else{
            //logger.info(key + " query does not have qrels");
        }
        
    }
    
    
    /**
     * run a set of queries
     * 
     * @throws ParseException
     * @throws IOException 
     */
    public void runQuerySet() throws ParseException, IOException{
        
        
        //wt2g b0 = 352
        //trec4_5 b0 = 240
        //gov2
        //ohsumed
        //wt10g
        
        // get to size of the inverted index 
        // if not already calculated
        
        if (SPUDLMSimilarity.b0est == true){
            SPUDLMSimilarity.b0 = estimateB0();
        }else{
            //else set to average unique terms doc length as a simple estimate
            //SPUDLMSimilarity.b0 = reader.getSumDocFreq("text")/reader.maxDoc();
            logger.info("Using estimated background model mass " + SPUDLMSimilarity.b0 );
        }

        
        //run the set of queries
        for (String key : this.current_set.keySet()){
            if (eval.getQrels().containsKey(key)){
                    runQuery(key);
                
            }else{
                //logger.info("No qrels for Query " + key);
            }
        }
        
        logger.log(Level.INFO, "MAP " + Mean(this.AP_values) + " P@10 " + Mean(this.PREC_values) + " NDCG10 " + 
                Mean(this.NDCG10_values) + " NDCG20 " + Mean(this.NDCG20_values) 
                + " for " + this.NDCG20_values.size() + " queries");
        
        
        clearMaps();
        System.out.println("");
    }
    
    
    public static void main(String[] args) throws ParseException, IOException{
        
        LoggerInitializer.setup();
        
        if (args.length >2){
            
            //set this to true to do the esimation of the DCM background parameter
            //else it will be set to the average document length
            SPUDLMSimilarity.b0est = false;
            
            //for SPUD LM
            SPUDLMSimilarity.b0 = 775;
            
            //for Dirichlet LM
            SPUDLMSimilarity.dir_mu = 3000;
            
            
            
            // Open the lucene index dir in args[0]
            QuerySearch i = new QuerySearch(args[0]);
            
            i.loadXMLTopics(args[1]);
            i.loadQrels(args[2]);

            
            //set the query type (summary, description, etc)
            int qtype = QuerySearch.desc;
            
            
            SPUDLMSimilarity.method = SPUDLMSimilarity.spud;
            SPUDLMSimilarity.omega = 0.85;            
            
            CustomQuery.QuerySmoothing = false;
            
            
            
            QueryExpansion.EXPAND = true;
            QueryExpansion.method = QueryExpansion.RM3;
            QueryExpansion.num_expansion_terms = 30;
            QueryExpansion.interpolation = 0.5;            
            QueryExpansion.pseudo_rel_docs = 5;

            logger.info("SPUD (0.85) DQM-No  RM3 (lambda=0.5) |F|=" + 5 + " E=30");
            i.setQuerySet(qtype);
            i.runQuerySet();


            //
            // use the discriminative query modelling approach
            // basically this re-weights query terms in longer queries
            //
            CustomQuery.QuerySmoothing = true;
            CustomQuery.QueryMethod = CustomQuery.QuerySPUD;
            CustomQuery.QueryParam = 0.85;
            
            
            //
            // turn on query expansion
                    
            
            QueryExpansion.EXPAND = true;
            QueryExpansion.method = QueryExpansion.RM3;
            QueryExpansion.num_expansion_terms = 30;
            QueryExpansion.interpolation = 0.5;            
            QueryExpansion.pseudo_rel_docs = 5;

            logger.info("SPUD (0.85) DQM-0.85  RM3 (lambda=0.5) |F|=" + 5 + " E=30");
            i.setQuerySet(qtype);
            i.runQuerySet();            

            
            //
            // QTM does not seem to be as effective for longer queries
            // as RM3 outperforms QTM for longer queries 
            // (the reverse is true for short keyword queries)
            //

            QueryExpansion.EXPAND = true;
            QueryExpansion.method = QueryExpansion.SPUDQTM;
            QueryExpansion.num_expansion_terms = 30;
            QueryExpansion.interpolation = 0.5;            
            QueryExpansion.pseudo_rel_docs = 5;

            logger.info("SPUD (0.85) DQM-0.85  QTM (lambda=0.5) |F|=" + 5 + " E=30");
            i.setQuerySet(qtype);
            i.runQuerySet();            
            
            
            
            

 
            i.reader.close();
            
            
        }else{
            logger.info("QueryIndex (index) (topics_file) (qrels_file) \n\n"
                    + "\t\"index\" is the lucene index directory\n"
                    + "\t\"topics_file\" is the trec topics file\n"
                    + "\t\"qrels\" is the qrels file (binary relevance is assumed)\n"
                    + "\n\n\tNote: The estimate of the background model is calculated each time. "
                    + "\n\t      It could be stored in the index once its calculated to save time.\n"
                    + "\t      Also note that the query effectiveness metrics are written to stdout\n"
                    + "\t      so they can be redirected to a file for analysis.");
        }
    }

    
}
