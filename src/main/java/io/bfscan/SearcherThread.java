package io.bfscan;

import java.util.Comparator;
import java.util.PriorityQueue;
import me.lemire.integercompression.*;
import io.bfscan.query.Query;

public class SearcherThread implements Runnable {
  public Thread Thr;
  private String name;
  private int [][] index;
  private Query query;
  private int thid;
  private int numhit;
  private int queryLength;
  private Score [] allScore;
  private int [] doclen;
  private float adl;
  private int [] dcomPostingLen;
  private final IntegerCODEC codec =  new Composition(new FastPFOR(), new VariableByte());
  
  float BM25(int tf, float idf, int dlen, float adl, float k1, float b) {
    float score = 0.0f;
    score = idf * ((k1+1.0f) * tf)/(k1*(1.0f-b+b*dlen/adl)+tf);
    return score;
  }
  
  public SearcherThread(String name, int [][] index, Query query, int thid, int numhit, Score [] allScore, 
      int [] doclen, float adl, int [] dcomPostingLen) {
    this.name = name;
    this.index = index;
    this.query = query;
    this.thid = thid;
    this.numhit = numhit;
    this.allScore = allScore;
    this.doclen = doclen;
    this.adl = adl;
    this.dcomPostingLen = dcomPostingLen;
    queryLength = query.TermID.size();
  }
  
  public void run() {
    int [][] posting = new int[queryLength][];
    int [] postingLen = new int[queryLength];
    float [] idf = new float[queryLength];
    
    /* uncompress posting lists for query words */
    for(int i = 0; i < queryLength; i++) {
      int tid = query.TermID.get(i);
      idf[i] = query.idf.get(i);
      int len = index[tid].length;
      posting[i] = new int[dcomPostingLen[tid]+1];
      IntWrapper outPos = new IntWrapper(0);
      codec.uncompress(index[tid], new IntWrapper(0), len, posting[i], outPos);
      postingLen[i] = outPos.intValue();
    }
    
    PriorityQueue<Score> topkQueue = new PriorityQueue<Score>(numhit, new Comparator<Score>() {
      public int compare(Score a, Score b) {
         if(a.score < b.score)
           return -1;
         else
           return 1;
      }
    });
    
    PriorityQueue<Score> mergerQueue = new PriorityQueue<Score>(queryLength+1, new Comparator<Score>() {
      public int compare(Score a, Score b) {
         if(a.docno < b.docno)
           return -1;
         else if(a.docno > b.docno)
           return 1;
         else
           return 0;
      }
    });
    
    int [] lastDocid = new int[queryLength];
    int [] curpos = new int[queryLength];
    /* initialize the queue  */
    for(int i = 0; i < queryLength; i++) {
      if(postingLen[i] > 0) {
        int id = posting[i][0];
        int tf = posting[i][1];
        float score = BM25(tf, idf[i], doclen[id], adl, 0.8f, 0.3f);
        mergerQueue.add(new Score(id, score, i));
        lastDocid[i] = id;
        curpos[i] = tf+2;
      }
    }
    
    int prevId = -1; 
    float scoreSum = 0.0f;
    int n = 0;
    while(!mergerQueue.isEmpty()) {
      Score s = mergerQueue.poll();
      if(s.docno == prevId) {
        scoreSum += s.score;
      }  
      else {
        if(prevId != -1) {
            if(n < numhit) {
              topkQueue.add(new Score(prevId, scoreSum, query.qno));
              n++;
            }
            else {
              if(topkQueue.peek().score < scoreSum) {
                topkQueue.poll();
                topkQueue.add(new Score(prevId, scoreSum, query.qno));
              }
            }
        }
        prevId = s.docno;
        scoreSum = s.score;
      }  
      int t = s.qid;
      if(curpos[t] < postingLen[t]) {
        int id = posting[t][curpos[t]] + lastDocid[t];
        int tf = posting[t][curpos[t]+1];
        float score = BM25(tf, idf[t], doclen[id], adl, 0.8f, 0.3f);
        mergerQueue.add(new Score(id, score, t));
        lastDocid[t] = id;
        curpos[t] += (tf+2);
      }
    }
    
    /* retrieve top k results  */
    int topkQSize = Math.min(n, topkQueue.size());
    int spos = numhit * thid;
    for(int k = 0; k < topkQSize; k++) {
      Score temp = topkQueue.poll();
      if(spos < allScore.length) {
        allScore[spos].docno = temp.docno;
        allScore[spos].score = temp.score;
        allScore[spos].qid = query.qno;
        spos++;
      }
    }
  }
  
  public void start () {
    if (Thr == null) {
       Thr = new Thread(this, name);
       Thr.start ();
    }
  }
  
}
