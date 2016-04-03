package io.bfscan;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

import io.bfscan.data.MTPForDocVector;

import io.bfscan.query.*;

public class SSComRankerThread implements Runnable {
  public Thread Thr;
  private String threadName;
  private int start, end;
  private ComKeyValue[] data;
  private Query query;
  private int numTopDoc;
  private int qlen;
  private int thid;
  private clusterInfo cluster;
  private Score [] allScore;
  private final MTPForDocVector doc = new MTPForDocVector();
  private int[] tf = new int[100];
  private int[] qtid = new int[100];
  private double[] idf =  new double[100];
  private float adl;
  
  public SSComRankerThread(String name, clusterInfo cluster, ComKeyValue[] data, Query query, 
      int numTopDoc, int thid, Score [] allScore, float adl) {
      threadName = name;
      this.cluster = cluster;
      this.data = data;
      this.numTopDoc = numTopDoc;
      this.query = query;
      this.thid = thid;
      this.adl = adl;
      this.allScore = allScore;
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
   
    float k1 = 1.0f; float b = 0.5f; float score = 0.0f;
    int dlen = 0;
    int curHeapSize = 0;
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
         Arrays.fill(tf, 0);
         doc.fromIntArrayWritable(data[i].doc, doc);
         dlen = doc.getLength();
         if(qlen == 1) {
           score = 0.0f;
           int tf0 = 0;
           for (int termid : doc.getTermIds()) {
             if(termid == qtid[0]) tf0++; 
           }
           if(tf0 > 0) score += idf[0] * ((k1+1.0f) * tf0)/(k1*(1.0f-b+b*dlen/adl)+tf0);
         }
         
         else if(qlen == 2) {
           score = 0.0f;
           int tf0 = 0, tf1 = 0;
             for (int termid : doc.getTermIds()) {
               if(termid == qtid[0]) tf0++; 
               else if(termid == qtid[1]) tf1++;
             }
             if(tf0 > 0) score += idf[0] * ((k1+1.0f) * tf0)/(k1*(1.0f-b+b*dlen/adl)+tf0);
             if(tf1 > 0) score += idf[1] * ((k1+1.0f) * tf1)/(k1*(1.0f-b+b*dlen/adl)+tf1);
         }
         
         else if(qlen == 3) {
             score = 0.0f;
             int tf0 = 0, tf1 = 0, tf2 = 0;
               for (int termid : doc.getTermIds()) {
                 if(termid == qtid[0]) tf0++; 
                 else if(termid == qtid[1]) tf1++;
                 else if(termid == qtid[2]) tf2++;
               }
               if(tf0 > 0) score += idf[0] * ((k1+1.0f) * tf0)/(k1*(1.0f-b+b*dlen/adl)+tf0);
               if(tf1 > 0) score += idf[1] * ((k1+1.0f) * tf1)/(k1*(1.0f-b+b*dlen/adl)+tf1);
               if(tf2 > 0) score += idf[2] * ((k1+1.0f) * tf2)/(k1*(1.0f-b+b*dlen/adl)+tf2);
         }
         
         else if(qlen == 4) {
           score = 0.0f;
           int tf0 = 0, tf1 = 0, tf2 = 0, tf3 = 0;
             for (int termid : doc.getTermIds()) {
               if(termid == qtid[0]) tf0++; 
               else if(termid == qtid[1]) tf1++;
               else if(termid == qtid[2]) tf2++;
               else if(termid == qtid[3]) tf3++;
             }
             if(tf0 > 0) score += idf[0] * ((k1+1.0f) * tf0)/(k1*(1.0f-b+b*dlen/adl)+tf0);
             if(tf1 > 0) score += idf[1] * ((k1+1.0f) * tf1)/(k1*(1.0f-b+b*dlen/adl)+tf1);
             if(tf2 > 0) score += idf[2] * ((k1+1.0f) * tf2)/(k1*(1.0f-b+b*dlen/adl)+tf2);
             if(tf3 > 0) score += idf[3] * ((k1+1.0f) * tf3)/(k1*(1.0f-b+b*dlen/adl)+tf3);
        }
         
        else if(qlen == 5) {
             score = 0.0f;
             int tf0 = 0, tf1 = 0, tf2 = 0, tf3 = 0, tf4 = 0;
               for (int termid : doc.getTermIds()) {
                 if(termid == qtid[0]) tf0++; 
                 else if(termid == qtid[1]) tf1++;
                 else if(termid == qtid[2]) tf2++;
                 else if(termid == qtid[3]) tf3++;
                 else if(termid == qtid[4]) tf4++;
               }
               if(tf0 > 0) score += idf[0] * ((k1+1.0f) * tf0)/(k1*(1.0f-b+b*dlen/adl)+tf0);
               if(tf1 > 0) score += idf[1] * ((k1+1.0f) * tf1)/(k1*(1.0f-b+b*dlen/adl)+tf1);
               if(tf2 > 0) score += idf[2] * ((k1+1.0f) * tf2)/(k1*(1.0f-b+b*dlen/adl)+tf2);
               if(tf3 > 0) score += idf[3] * ((k1+1.0f) * tf3)/(k1*(1.0f-b+b*dlen/adl)+tf3);
               if(tf4 > 0) score += idf[4] * ((k1+1.0f) * tf4)/(k1*(1.0f-b+b*dlen/adl)+tf4);
         }
         
        else {
          Arrays.fill(tf, 0);
          score = 0.0f;
          for (int termid : doc.getTermIds()) {
            for(int k = 0; k < qlen; k++) {
              if(termid == qtid[k])
                tf[k]++;
            }
          }
          for(int k = 0; k < qlen; k++) {
            if(tf[k] > 0)
                 score += idf[k] * ((k1+1.0f) * tf[k])/(k1*(1.0f-b+b*dlen/adl)+tf[k]);
          }
        }
         
         if(score <= 0.0f)
           continue;
         
         if(curHeapSize < numTopDoc) {
           scoreQueue.add(new Score(i, score, query.qno));
             curHeapSize++;
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
     int scoreQSize = Math.min(scoreQueue.size(), numTopDoc);
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
        Thr = new Thread (this, threadName);
        Thr.start ();
     }
  }

}
