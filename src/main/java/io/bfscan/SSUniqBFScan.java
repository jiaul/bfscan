package io.bfscan;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import tl.lin.data.array.IntArrayWritable;

import io.bfscan.dictionary.*;
import io.bfscan.data.PForDocVector;
import io.bfscan.data.TermStatistics;

import java.util.*;

import io.bfscan.query.*;

public class SSUniqBFScan {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private SSUniqBFScan() {}
  public static String [] keys;
  public static int [][] docs;
  public static int [][] freq;
  public static int [] doclen;
  public static int numDocRead = 0;
  private static final PForDocVector DOC = new PForDocVector();
  private static SSUniqRankerThread [] Thread;
  public static Score [] allScore;
  public static HashMap<String, Integer> clusAssign = new HashMap<String, Integer>();
  public static int [] clusCard = new int [10000];
  public static int [] cumClusCard = new int [10000];
  public static int numOfClus = 0;
  public static int [] clusterBoundIndex =  new int[10000];
  public static clusterInfo cluster = new clusterInfo();
  public static float adl;
  public static void main(String[] args) throws IOException {
    if (args.length < 10) {
      System.out.println("args: [doc. vectors path] [# top doc] [dictionary path] [# thread] [query file] [# doc in collection]");
      System.out.println("[cluster assignment] [cluster centers] [word2vec query] [# cluster for Sel. Search]");
      System.exit(-1);
    }

    String docvecPath = args[0];
    String queryFile = args[4];
    int numTopDoc = Integer.parseInt(args[1]);
    int numThread = Integer.parseInt(args[3]);
    allScore = new Score[numTopDoc * numThread];
    int numDoc = Integer.parseInt(args[5]);
    int numTopClus = Integer.parseInt(args[9]);
    keys = new String[numDoc];
    docs = new int[numDoc][];
    freq = new int[numDoc][];
    doclen = new int[numDoc];
    // load dictionary
    String dictPath = args[2];
    Configuration conf = new Configuration();
    FileSystem fs1 = FileSystem.get(conf); 
    DefaultFrequencySortedDictionary dictionary = new DefaultFrequencySortedDictionary(dictPath, fs1);
    TermStatistics stats = new TermStatistics(new Path(dictPath), fs1);
    // read query and convert to ids
    queryTermtoID allQuery = new queryTermtoID(queryFile, dictionary);
    
    System.out.println("Number of query read: " + allQuery.nq);
    Arrays.fill(clusCard, 0);
    // reading cluster assignments
    try {
      BufferedReader in = new BufferedReader(new FileReader(args[6]));
      String line;
      while((line = in.readLine()) != null) {
        String [] s = line.split("\\s+");
        int cno = Integer.parseInt(s[1]);
        clusAssign.put(s[0], cno);
        clusCard[cno]++;
      }
      in.close();
    }
    catch(IOException e) {
      
    }
    
    //reading cluster centers
    double [][] clusCenters = new double [1000][];
    try {
      BufferedReader in = new BufferedReader(new FileReader(args[7]));
      String line;
      while((line = in.readLine()) != null) {
        String [] s = line.split(",");
        clusCenters[numOfClus] = new double [s.length];
        for(int i = 0; i < s.length; i++)
          clusCenters[numOfClus][i] = Double.parseDouble(s[i]);
        numOfClus++;
      }
      in.close();
    }
    catch(IOException e) {
      
    }
    
    Arrays.fill(cumClusCard, 0);
    cumClusCard[0] = clusCard[0];
    clusterBoundIndex[0] = 0;
    for(int i = 1; i < numOfClus; i++) {
      cumClusCard[i] = clusCard[i] + cumClusCard[i-1];
      clusterBoundIndex[i] = clusCard[i-1] + clusterBoundIndex[i-1];
    }  
    
    // reading word2vec queries
    int [] qno = new int [10000];
    double [][] queryvec = new double [10000][];
    int numOfQuery = 0;
    try {
      BufferedReader in = new BufferedReader(new FileReader(args[8]));
      String line;
      while((line = in.readLine()) != null) {
        String [] s = line.split("\\s+");
        queryvec[numOfQuery] = new double [s.length-1];
        qno[numOfQuery] = Integer.parseInt(s[0]);
        for(int i = 1; i < s.length; i++)
          queryvec[numOfQuery][i-1] = Double.parseDouble(s[i]);
        numOfQuery++;
      }
      in.close();
    }
    catch(IOException e) {
      
    }
    
    /* rank query specific clusters */
    HashMap<Integer, int []> queryDepClus = new HashMap<Integer, int []>();
    double [] simScore = new double[numOfClus];
    for(int i = 0; i < numOfQuery; i++) {
      for(int j = 0; j < numOfClus; j++)
        simScore[j] = io.bfscan.util.Similarity.cosine(queryvec[i], clusCenters[j]);
      int [] temp = new int[numOfClus];
      for(int k = 0; k < numOfClus; k++) {
        double sim = -2.0;
        int bestIndex = -1;
        for(int l = 0; l < numOfClus; l++)
          if(sim < simScore[l]) {
            sim = simScore[l];
            bestIndex = l;
          }
        temp[k] = bestIndex;
        simScore[bestIndex] = -100.0;
      }
      queryDepClus.put(qno[i], temp);
    }
    
    int max = Integer.MAX_VALUE;
    
    FileSystem fs = FileSystem.get(new Configuration());
    Path p = new Path(docvecPath);

    if (fs.getFileStatus(p).isDirectory()) {
      numDoc = readSequenceFilesInDir(p, fs, max);
    } else {
      numDoc = readSequenceFile(p, fs, max);
    }
    System.out.println("No. of doc read: " + numDoc);
    
    adl = stats.getCollectionSize()/numDoc;
    /* compute ctf and idf of query terms */
    for(int k = 0; k < allQuery.nq; k++) {
      List<Float> idf = new ArrayList<Float>();
      List<Long> ctf = new ArrayList<Long>();
      for(int l = 0; l < allQuery.query[k].TermID.size(); l++) {
        int id = allQuery.query[k].TermID.get(l);
        int df = stats.getDf(id);
        idf.add((float) Math.log((1.0f*numDoc-df+0.5f)/(df+0.5f)));
        ctf.add(stats.getCf(id));
      }
      allQuery.query[k] = new Query(allQuery.query[k].qno, allQuery.query[k].TermID, idf, ctf);
    }
    
    cluster.numOfTopClus = numTopClus;
    cluster.cumClusCard = cumClusCard;
    cluster.numOfThread = numThread;
    cluster.clusCard = clusCard;
    cluster.clusterBoundIndex = clusterBoundIndex;
    
    Thread = new SSUniqRankerThread[numThread];
    
    for(int i = 0; i < allScore.length; i++)
      allScore[i] = new Score(-1, -1000.0f, 0);
    
      long startTime = System.nanoTime(); 
      for(int j = 0; j < allQuery.nq; j++) {
      cluster.clusters = queryDepClus.get(allQuery.query[j].qno);  
      for(int i = 0; i < numThread; i++) {
        Thread[i] = new SSUniqRankerThread("Thread", cluster, docs, freq, doclen, allQuery.query[j], numTopDoc, i, allScore, adl);
        Thread[i].start();
      }
    
      try {
      for(int i = 0; i < numThread; i++)
        Thread[i].Thr.join();
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
          System.out.print(allScore[l].qid+" Q0 "+keys[allScore[l].docno]+" "+l+" ");
          System.out.println(allScore[l].score+" bm25-ssuniq"); 
          allScore[l].score = -1000.0f;
        }
     }
    
      long endTime = System.nanoTime(); 
      System.out.println("Time: " + (endTime-startTime)/1000000000.0+" s | # of query: "+allQuery.nq);
  }
  
  

  private static int readSequenceFile(Path path, FileSystem fs, int max) throws IOException {
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, fs.getConf());
    try {
         reader.getKeyClass().toString();
         reader.getValueClass().toString();
    } catch (Exception e) {
      throw new RuntimeException("Error: loading key/value class");
    }

    Writable key;
    IntArrayWritable value;
    int n = 0;
    Integer c;
    
    try {
      if ( Tuple.class.isAssignableFrom(reader.getKeyClass())) {
        key = TUPLE_FACTORY.newTuple();
      } else {
        key = (Writable) reader.getKeyClass().newInstance();
      }

      if ( Tuple.class.isAssignableFrom(reader.getValueClass())) {
        value = (IntArrayWritable) TUPLE_FACTORY.newTuple();
      } else {
        value = (IntArrayWritable) reader.getValueClass().newInstance();
      }

      while (reader.next(key, value)) {
        PForDocVector.fromIntArrayWritable(value, DOC);
        c = clusAssign.get(key.toString());
        if(c != null) {
          int i = c.intValue();
          keys[clusterBoundIndex[i]] = key.toString();
          doclen[clusterBoundIndex[i]] = DOC.getLength();
          int [] termid = DOC.getTermIds();
          Map<Integer,List<Integer>> map = new HashMap<Integer,List<Integer>>();
          for(int j = 0; j < termid.length; j++) {
            if(map.get(termid[j]) == null) 
              map.put(termid[j], new ArrayList<Integer>());
            map.get(termid[j]).add(j);  
          }
          Object [] keys = map.keySet().toArray();
          Arrays.sort(keys);
          docs[clusterBoundIndex[i]] = new int[keys.length];
          freq[clusterBoundIndex[i]] = new int[keys.length];
          for(int k = 0; k < keys.length; k++) {
            List<Integer> temp = map.get(keys[k]);
            docs[clusterBoundIndex[i]][k] = (int) keys[k];
            freq[clusterBoundIndex[i]][k] = temp.size();
          }
          clusterBoundIndex[i]++;
          numDocRead++;
        }
        
        n++;

        if (n >= max)
          break;
      }
      reader.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return n;
  }

  private static int readSequenceFilesInDir(Path path, FileSystem fs, int max) {
    int n = 0;
    try {
      FileStatus[] stat = fs.listStatus(path);
      for (int i = 0; i < stat.length; ++i) {
        n += readSequenceFile(stat[i].getPath(), fs ,max);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return n;
  }
  
}

