package io.bfscan;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

import io.bfscan.query.*;

public class SSUniqRankerThread implements Runnable {
  public Thread Thr;
  private String threadName;
  private int start, end;
  private Query query;
  private int numTopDoc;
  private int qlen;
  private int thid;
  private float adl;
  private clusterInfo cluster;
  private int [][] docs;
  private int [][] freq;
  private Score [] allScore;
  private int [] doclen;
  private int[] qtid = new int[100];
  private double[] idf =  new double[100];
  
  public SSUniqRankerThread(String name, clusterInfo cluster,  int [][] docs, int [][] freq,
       int [] doclen, Query query, int numTopDoc, int thid, Score [] allScore, float adl) {
      threadName = name;
      this.docs = docs;
      this.freq = freq;
      this.doclen = doclen;
      this.numTopDoc = numTopDoc;
      this.query = query;
      this.thid = thid;
      this.allScore = allScore;
      this.cluster = cluster;
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
    
    int n = 0;
    float k1 = 1.0f;
    float b = 0.5f;
    float score = 0.0f;
    float mc1 = k1*(1.0f-b);
    float mc2 = (k1*b)/adl;
    
    for(int c = 0; c < cluster.numOfTopClus; c++) {
        int cid = cluster.clusters[c];
        int offset;
        if(cid == 0)
          offset = 0;
        else
          offset = cluster.cumClusCard[cid-1];
        int chunkSize = (cluster.clusterBoundIndex[cid]-offset)/cluster.numOfThread;
        start = offset + thid * chunkSize;
        if(thid != (cluster.numOfThread - 1))
          end = start + chunkSize;
        else
          end = cluster.clusterBoundIndex[cid];
       for(int i = start; i < end; i++) {
        score = 0.0f; 
        float lenFac = mc2 * doclen[i];
        
        if(qlen == 3) {
          int p = Arrays.binarySearch(docs[i], qtid[0]);
          if(p >= 0) 
            score += (idf[0] * freq[i][p])/(mc1 + lenFac + freq[i][p]);
          
          p = Arrays.binarySearch(docs[i], qtid[1]);
          if(p >= 0) 
            score += (idf[1] * freq[i][p])/(mc1 + lenFac + freq[i][p]);
          
          p = Arrays.binarySearch(docs[i], qtid[2]);
          if(p >= 0) 
            score += (idf[2] * freq[i][p])/(mc1 + lenFac + freq[i][p]);
        }
        
        else if(qlen == 4) {
          int p = Arrays.binarySearch(docs[i], qtid[0]);
          if(p >= 0) 
            score += (idf[0] * freq[i][p])/(mc1 + lenFac + freq[i][p]);
          
          p = Arrays.binarySearch(docs[i], qtid[1]);
          if(p >= 0) 
            score += (idf[1] * freq[i][p])/(mc1 + lenFac + freq[i][p]);
          
          p = Arrays.binarySearch(docs[i], qtid[2]);
          if(p >= 0) 
            score += (idf[2] * freq[i][p])/(mc1 + lenFac + freq[i][p]);
          
          p = Arrays.binarySearch(docs[i], qtid[3]);
          if(p >= 0) 
            score += (idf[3] * freq[i][p])/(mc1 + lenFac + freq[i][p]);
        }
        
        else if(qlen == 2) {
          int p = Arrays.binarySearch(docs[i], qtid[0]);
          if(p >= 0) 
            score += (idf[0] * freq[i][p])/(mc1 + lenFac + freq[i][p]);
          
          p = Arrays.binarySearch(docs[i], qtid[1]);
          if(p >= 0) 
            score += (idf[1] * freq[i][p])/(mc1 + lenFac + freq[i][p]);
        }
        
        else if(qlen == 1) {
          int p = Arrays.binarySearch(docs[i], qtid[0]);
          if(p >= 0)
                score += (idf[0] * freq[i][p])/(mc1 + lenFac + freq[i][p]);
        }
        
        else if(qlen == 5) {
          int p = Arrays.binarySearch(docs[i], qtid[0]);
          if(p >= 0) 
            score += (idf[0] * freq[i][p])/(mc1 + lenFac + freq[i][p]);
          
          p = Arrays.binarySearch(docs[i], qtid[1]);
          if(p >= 0) 
            score += (idf[1] * freq[i][p])/(mc1 + lenFac + freq[i][p]);
          
          p = Arrays.binarySearch(docs[i], qtid[2]);
          if(p >= 0) 
            score += (idf[2] * freq[i][p])/(mc1 + lenFac + freq[i][p]);
          
          p = Arrays.binarySearch(docs[i], qtid[3]);
          if(p >= 0) 
            score += (idf[3] * freq[i][p])/(mc1 + lenFac + freq[i][p]);
          
          p = Arrays.binarySearch(docs[i], qtid[4]);
          if(p >= 0) 
            score += (idf[4] * freq[i][p])/(mc1 + lenFac + freq[i][p]);
        }
        
        else {
          for(int j = 0; j < qlen; j++) {
             int p = Arrays.binarySearch(docs[i], qtid[j]);
             if(p >= 0) 
               score += (idf[j] * freq[i][p])/(mc1 + lenFac + freq[i][p]);
          }
        }
        
        if(score <= 0.0f)
          continue;
        
        if(n < numTopDoc) {
           scoreQueue.add(new Score(i, score, query.qno));
           n++;
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
     int scoreQSize = Math.min(n, scoreQueue.size());
     int spos = numTopDoc * thid;
     for(int k = 0; k < scoreQSize; k++) {
       Score temp = scoreQueue.poll();
       allScore[spos].docno = temp.docno;
       allScore[spos].qid = temp.qid;
       allScore[spos].score = temp.score;
       //allScore[spos] = new Score(temp.docid, temp.score, temp.qid);
       spos++;
     }
  }
  
  public void start () {
     if (Thr == null) {
        Thr = new Thread (this, threadName);
        Thr.start ();
     }
  }
}