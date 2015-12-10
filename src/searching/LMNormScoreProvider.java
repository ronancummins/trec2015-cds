package searching;

import java.io.IOException;
import java.util.logging.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.CustomScoreProvider;
import org.apache.lucene.search.similarities.SPUDLMSimilarity;


/**
 *
 * This provides the correct normalisation for language models that use 
 * Bayesian smoothing (i.e. the SPUD model 
 * and the LM with Dirichlet priors smoothing)
 * 
 * @author ronanc
 */
public class LMNormScoreProvider extends CustomScoreProvider {

    private final static Logger logger = Logger.getLogger(LMNormScoreProvider.class.getName());
    
    private int queryLen;
    
    
    public LMNormScoreProvider(LeafReaderContext context, int _queryLen) {
        super(context);
        queryLen = _queryLen;
    }

    public float customScore(int doc,
            float subQueryScore,
            float[] valSrcScores)
            throws IOException {

        //get the document length
        float dl = this.context.reader().getNumericDocValues("TotalTerms").get(doc);
        float dvl = this.context.reader().getNumericDocValues("UniqueTerms").get(doc);
        float ent = this.context.reader().getNumericDocValues("Entropy").get(doc);
        float lmnorm;
        
        if (SPUDLMSimilarity.method == SPUDLMSimilarity.dir){
            //LM Dirichlet 
            
            lmnorm = (float) (Math.log(SPUDLMSimilarity.dir_mu / (dl + SPUDLMSimilarity.dir_mu)));
        }else if (SPUDLMSimilarity.method == SPUDLMSimilarity.spud){
            //spud
            double spud_mu = SPUDLMSimilarity.b0*SPUDLMSimilarity.omega/(1-SPUDLMSimilarity.omega);
            lmnorm = (float) (Math.log(spud_mu / (dvl + spud_mu)));
            
        }else if (SPUDLMSimilarity.method == SPUDLMSimilarity.spud_entropy){
            //spud
            double spud_mu = SPUDLMSimilarity.b0*SPUDLMSimilarity.omega/(1-SPUDLMSimilarity.omega);
            lmnorm = (float) (Math.log(spud_mu / (ent + spud_mu)));
            
            
        }else{
            double spud_mu = SPUDLMSimilarity.b0*SPUDLMSimilarity.omega/(1-SPUDLMSimilarity.omega);
            lmnorm = (float) (Math.log(spud_mu / (dvl + spud_mu)));
            
        }
        
        
        
        return (subQueryScore + lmnorm );
        
    }

}
