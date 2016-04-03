package io.bfscan;


import io.bfscan.query.Query;
import io.bfscan.query.queryTermtoID;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.EOFException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import io.bfscan.data.TermStatistics;
import io.bfscan.dictionary.DefaultFrequencySortedDictionary;

public class SearchSplitIndex {
  
  public static int [] [] [] index;
  public static int [] [] comPostingLen;
  public static int [] [] dcomPostingLen;
  public static String [] docid;
  public static int [] doclen;
  public static int numOfDoc, numOfSplit;
  public static SearcherThread [] Thread;
  public static Score [] allScore;
  public static float adl;
  public static int numTopDoc;
  
  public static void main(String[] args) throws IOException {
    if (args.length < 4) {
      System.out.println("args: [index path] [dictionary path] [query file] [# documents to return] ");
      System.exit(-1);
    }
    
    String indexPath = args[0];
    String dictPath = args[1];
    String queryFile = args[2];
    numTopDoc = Integer.parseInt(args[3]);
    
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf); 
    DefaultFrequencySortedDictionary dictionary = new DefaultFrequencySortedDictionary(dictPath, fs);
    TermStatistics termstats = new TermStatistics(new Path(dictPath), fs);
    int vocabsize = termstats.getVocabularySize()+1;
    
    queryTermtoID allQuery = new queryTermtoID(queryFile, dictionary);
    
    /* reading stat */
    ObjectInputStream statStream = new ObjectInputStream(new FileInputStream(indexPath + "/stat"));
    numOfDoc = (int) statStream.readInt();
    numOfSplit = (int) statStream.readInt();
    statStream.close();
    adl = termstats.getCollectionSize()/numOfDoc;
    
    /* compute ctfs and idfs of query terms  */
    for(int k = 0; k < allQuery.nq; k++) {
      List<Float> idf = new ArrayList<Float>();
      List<Long> ctf = new ArrayList<Long>();
      for(int l = 0; l < allQuery.query[k].TermID.size(); l++) {
        int id = allQuery.query[k].TermID.get(l);
        int df = termstats.getDf(id);
        idf.add((float) Math.log((1.0f*numOfDoc-df+0.5f)/(df+0.5f)));
        ctf.add(termstats.getCf(id));
      }
      allQuery.query[k] = new Query(allQuery.query[k].qno, allQuery.query[k].TermID, idf, ctf);
    }
    
    /* reading posting list lengths */
    comPostingLen = new int[numOfSplit][];
    dcomPostingLen = new int[numOfSplit][];
    for(int i = 0; i < numOfSplit; i++) {
      String path = indexPath + "/posting.meta." + i;
      ObjectInputStream ofStream = new ObjectInputStream(new FileInputStream(path));
      try {
        comPostingLen[i] = (int []) ofStream.readObject();
        dcomPostingLen[i] = (int []) ofStream.readObject();
      }
      catch (Exception ex) {
        ex.printStackTrace();
     }
     ofStream.close(); 
    }
    
    /* reading indexes */
    index = new int[numOfSplit][vocabsize][];
    for(int i = 0; i < numOfSplit; i++) {
      String path = indexPath + "/posting." + i;
      ObjectInputStream postingStream = new ObjectInputStream(new FileInputStream(path));
      try {
        for(int j = 0; j < vocabsize; j++) {
          index[i][j] = new int[comPostingLen[i][j]];
              for(int k = 0; k < comPostingLen[i][j]; k++)
                index[i][j][k] = postingStream.readInt();
        }
      }
      catch (Exception ex) {
        ex.printStackTrace();
      }
      postingStream.close();
    }
    
    /* reading document information */
    doclen = new int[numOfDoc];
    docid = new String[numOfDoc];
    ObjectInputStream docidStream = new ObjectInputStream(new FileInputStream(indexPath + "/docid"));
    int id;
    while(true) {
      try {
        id = (int) docidStream.readObject();
        docid[id] = (String) docidStream.readObject();
        doclen[id] = (int) docidStream.readObject();
      }
      catch (ClassNotFoundException ex) {
        System.err.println("A ClassNotFoundException was caught: " + ex.getMessage());
        ex.printStackTrace();
      }
      catch(EOFException e) {
        break;
      }
    }
    docidStream.close();
    
    Thread = new SearcherThread[numOfSplit];
    allScore = new Score[numOfSplit * numTopDoc];
    /* initialize score array */
    for(int i = 0; i < allScore.length; i++)
      allScore[i] = new Score(-1, -100.0f, -1);
    
    long startTime = System.nanoTime();
    
    for(int i = 0; i < allQuery.nq; i++) {
          for(int j = 0; j < numOfSplit; j++) {
            String name = "thread-" + j;
            Thread[j] = new SearcherThread(name, index[j], allQuery.query[i], j, 
                        numTopDoc, allScore, doclen, adl, dcomPostingLen[j]);
            Thread[j].start();
          }
          try {
            for(int j = 0; j < numOfSplit; j++)
              Thread[j].Thr.join();
          }
          catch (InterruptedException e) {
              System.out.println("Main thread Interrupted");
          }
        
          Arrays.sort(allScore, new Comparator<Score>() {
          public int compare(Score a, Score b) {
            if(a.score > b.score)
              return -1;
            else if(a.score < b.score)
              return 1;
            else
              return 0;
         }
       });
        
        for(int l = 0; l < numTopDoc; l++) {
          System.out.print(allScore[l].qid + " Q0 " + docid[allScore[l].docno] + " ");
          System.out.println(l + " " + allScore[l].score + " bm25-split-index"); 
          allScore[l].score = -100.0f;
        }
    }
    
    long endTime = System.nanoTime(); 
    double T = (endTime-startTime)/1000000000.0;
    DecimalFormat df = new DecimalFormat("#.##");
    System.out.println("Time: " + df.format(T) + " sec || # query: " + allQuery.nq + " || # split: " + numOfSplit);
  }  
}
