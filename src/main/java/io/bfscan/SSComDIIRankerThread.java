package io.bfscan;

import java.util.Comparator;
import java.util.PriorityQueue;
import me.lemire.integercompression.Composition;
import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.IntegerCODEC;
import me.lemire.integercompression.VariableByte;
import io.bfscan.query.*;

public class SSComDIIRankerThread implements Runnable {
  public Thread Thr;
  private String name;
  private Query query;
  private int numTopDoc, numTopClus, numThread;
  private int qlen;
  private int thid;
  private float adl;
  private clusterStat clusMetaData;
  private Score [] allScore;
  private ComDII [] DII;
  private int [] qtid = new int[100];
  private double [] idf =  new double[100];
  private int [] term = new int[50000];
  private int [] freq = new int[50000];
  private final IntegerCODEC codec =  new Composition(new FastPFOR(), new VariableByte());
  
  int bsearch(int [] data, int cand, int len) {
    int l = 0, r = len-1, m = 0;
    while(l <= r) {
      m = (l+r) >> 1;
      if(data[m] == cand)
        return m;
      else if(cand < data[m])
        r = m-1;
      else
        l = m+1;
    }
    
    return -1;
  }
  
  public SSComDIIRankerThread(String name, clusterStat clusMetaData, ComDII [] DII, Query query,  
         int numTopDoc, int thid, Score [] allScore, float adl, int numTopClus, int numThread) {
      this.name = name;
      this.DII = DII;
      this.numTopDoc = numTopDoc;
      this.query = query;
      this.thid = thid;
      this.allScore = allScore;
      this.clusMetaData = clusMetaData;
      this.numTopClus = numTopClus;
      this.numThread = numThread;
      this.adl = adl;
      qlen = query.TermID.size();
      for(int i = 0; i < qlen; i++) {
        idf[i] = query.idf.get(i);
        qtid[i] = query.TermID.get(i);
      }
  }
  
  public void run() {
    PriorityQueue<Score> scoreQueue = new PriorityQueue<Score>(numTopDoc, new Comparator<Score>() {
      public int compare(Score a, Score b) {
         if(a.score < b.score)
           return -1;
         else
           return 1;
      }
    });
    
    int curQsize = 0;
    float k1 = 1.0f;
    float b = 0.5f;
    float score = 0.0f;
    float mc1 = k1*(1.0f-b);
    float mc2 = (k1*b)/adl;
    int start = 0, end = 0, csz = 0;
    IntWrapper outPos = new IntWrapper(0);
    
    for(int c = 0; c < numTopClus; c++) {
        int cid = clusMetaData.clusRankList[c];
        csz = (int) Math.ceil((double)(clusMetaData.clusEndIndex[cid]-clusMetaData.clusStartIndex[cid])/numThread);
        start = clusMetaData.clusStartIndex[cid] + thid * csz;
        end = Math.min(start+csz, clusMetaData.clusEndIndex[cid]);
        
        for(int i = start; i < end; i++) {
          outPos.set(0);
          codec.uncompress(DII[i].term, new IntWrapper(0), DII[i].term.length, term, outPos);
          score = 0.0f; 
          int ulen = DII[i].ulen;
          float lenFac = mc2 * DII[i].len;
          boolean isFreqUncom = false;
          
          if(qlen == 3) {
            int p = bsearch(term, qtid[0], ulen);
            if(p >= 0) {
              outPos.set(0);
              codec.uncompress(DII[i].freq, new IntWrapper(0), DII[i].freq.length, freq, outPos);
              isFreqUncom = true;
              score += (idf[0] * freq[p])/(mc1 + lenFac + freq[p]);
            }
            p = bsearch(term, qtid[1], ulen);
            if(p >= 0) {
              if(!isFreqUncom) {
                outPos.set(0);
                codec.uncompress(DII[i].freq, new IntWrapper(0), DII[i].freq.length, freq, outPos);
                isFreqUncom = true;
              }
              score += (idf[1] * freq[p])/(mc1 + lenFac + freq[p]);
            }
            p = bsearch(term, qtid[2], ulen);
            if(p >= 0) {
              if(!isFreqUncom) {
                outPos.set(0);
                codec.uncompress(DII[i].freq, new IntWrapper(0), DII[i].freq.length, freq, outPos);
              }
              score += (idf[2] * freq[p])/(mc1 + lenFac + freq[p]);
            }
          }
          
          else if(qlen == 4) {
            int p = bsearch(term, qtid[0], ulen);
            if(p >= 0) {
              outPos.set(0);
              codec.uncompress(DII[i].freq, new IntWrapper(0), DII[i].freq.length, freq, outPos);
              isFreqUncom = true;
              score += (idf[0] * freq[p])/(mc1 + lenFac + freq[p]);
            }
            p = bsearch(term, qtid[1], ulen);
            if(p >= 0) {
              if(!isFreqUncom) {
                outPos.set(0);
                codec.uncompress(DII[i].freq, new IntWrapper(0), DII[i].freq.length, freq, outPos);
                isFreqUncom = true;
              }
              score += (idf[1] * freq[p])/(mc1 + lenFac + freq[p]);
            }
            p = bsearch(term, qtid[2], ulen);
            if(p >= 0) {
              if(!isFreqUncom) {
                outPos.set(0);
                codec.uncompress(DII[i].freq, new IntWrapper(0), DII[i].freq.length, freq, outPos);
                isFreqUncom = true;
              }
              score += (idf[2] * freq[p])/(mc1 + lenFac + freq[p]);
            }
            p = bsearch(term, qtid[3], ulen);
            if(p >= 0) {
              if(!isFreqUncom) {
                outPos.set(0);
                codec.uncompress(DII[i].freq, new IntWrapper(0), DII[i].freq.length, freq, outPos);
              }
              score += (idf[3] * freq[p])/(mc1 + lenFac + freq[p]);
            }
          }
          
          else if(qlen == 2) {
            int p = bsearch(term, qtid[0], ulen);
            if(p >= 0) {
              outPos.set(0);
              codec.uncompress(DII[i].freq, new IntWrapper(0), DII[i].freq.length, freq, outPos);
              isFreqUncom = true;
              score += (idf[0] * freq[p])/(mc1 + lenFac + freq[p]);
            }
            p = bsearch(term, qtid[1], ulen);
            if(p >= 0) {
              if(!isFreqUncom) {
                outPos.set(0);
                codec.uncompress(DII[i].freq, new IntWrapper(0), DII[i].freq.length, freq, outPos);
              }
              score += (idf[1] * freq[p])/(mc1 + lenFac + freq[p]);
            }
          }
          
          else if(qlen == 1) {
            int p = bsearch(term, qtid[0], ulen);
            if(p >= 0) {
              outPos.set(0);
              codec.uncompress(DII[i].freq, new IntWrapper(0), DII[i].freq.length, freq, outPos);
              score += (idf[0] * freq[p])/(mc1 + mc2 * DII[i].len + freq[p]);
            }
          }
          
          else if(qlen == 5) {
            int p = bsearch(term, qtid[0], ulen);
            if(p >= 0) {
              outPos.set(0);
              codec.uncompress(DII[i].freq, new IntWrapper(0), DII[i].freq.length, freq, outPos);
              isFreqUncom = true;
              score += (idf[0] * freq[p])/(mc1 + lenFac + freq[p]);
            }
            p = bsearch(term, qtid[1], ulen);
            if(p >= 0) {
              if(!isFreqUncom) {
                outPos.set(0);
                codec.uncompress(DII[i].freq, new IntWrapper(0), DII[i].freq.length, freq, outPos);
                isFreqUncom = true;
              }
              score += (idf[1] * freq[p])/(mc1 + lenFac + freq[p]);
            }
            p = bsearch(term, qtid[2], ulen);
            if(p >= 0) {
              if(!isFreqUncom) {
                outPos.set(0);
                codec.uncompress(DII[i].freq, new IntWrapper(0), DII[i].freq.length, freq, outPos);
                isFreqUncom = true;
              }
              score += (idf[2] * freq[p])/(mc1 + lenFac + freq[p]);
            }
            p = bsearch(term, qtid[3], ulen);
            if(p >= 0) {
              if(!isFreqUncom) {
                outPos.set(0);
                codec.uncompress(DII[i].freq, new IntWrapper(0), DII[i].freq.length, freq, outPos);
                isFreqUncom = true;
              }
              score += (idf[3] * freq[p])/(mc1 + lenFac + freq[p]);
            }
            p = bsearch(term, qtid[4], ulen);
            if(p >= 0) {
              if(!isFreqUncom) {
                outPos.set(0);
                codec.uncompress(DII[i].freq, new IntWrapper(0), DII[i].freq.length, freq, outPos);
              }
              score += (idf[4] * freq[p])/(mc1 + lenFac + freq[p]);
            }
          }
          
          else {
            for(int j = 0; j < qlen; j++) {
               int p = bsearch(term, qtid[j], ulen);
               if(p >= 0) {
                 if(!isFreqUncom) {
                   outPos.set(0);
                   codec.uncompress(DII[i].freq, new IntWrapper(0), DII[i].freq.length, freq, outPos);
                   isFreqUncom = true;
                 }
                 score += (idf[j] * freq[p])/(mc1 + lenFac + freq[p]);
               }
            }
          }
        
        if(score <= 0.0f)
          continue;
        
        if(curQsize < numTopDoc) {
           scoreQueue.add(new Score(i, score, query.qno));
           curQsize++;
        }
        else {
          if(scoreQueue.peek().score < score) {
            scoreQueue.poll();
            scoreQueue.add(new Score(i, score, query.qno));
          }
        }
       }
    } 
    
    /* take top k results  */
    int scoreQSize = scoreQueue.size();
    int spos = numTopDoc * thid;
    for(int k = 0; k < scoreQSize; k++) {
      Score temp = scoreQueue.poll();
      allScore[spos].docno = temp.docno;
      allScore[spos].score = temp.score;
      allScore[spos].qid = temp.qid;
      spos++;
    }
  }
  
  public void start () {
     if (Thr == null) {
        Thr = new Thread(this, name);
        Thr.start ();
     }
  }
}
