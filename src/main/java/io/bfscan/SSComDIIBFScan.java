package io.bfscan;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import me.lemire.integercompression.Composition;
import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.IntegerCODEC;
import me.lemire.integercompression.VariableByte;

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

public class SSComDIIBFScan {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private SSComDIIBFScan() {}
  public static int numDocRead = 0;
  public static int numFile = 0;
  public static ComDII [] DII;
  private static final PForDocVector DOC = new PForDocVector();
  private static SSComDIIRankerThread [] Thread;
  public static Score [] allScore;
  public static HashMap<String, Integer> clusAssign = new HashMap<String, Integer>();
  public static int numOfClus = 0;
  public static clusterStat clusMetaData = new clusterStat();
  public static float adl;
  public static int [] clusStartIndex;
  public static int [] clusEndIndex;
  public static int [] comDIIBuf = new int[500000];
  public static int [] diiBuf = new int[500000];
  public static final IntegerCODEC codec =  new Composition(new FastPFOR(), new VariableByte()); 
  public static void main(String[] args) throws IOException {
    if (args.length < 10) {
      System.out.println("args: [doc. vectors] [# top doc] [dictionary] [# thread] [query file] [# doc in coll.]");
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
    /* load dictionary  */
    String dictPath = args[2];
    Configuration conf = new Configuration();
    FileSystem fs1 = FileSystem.get(conf); 
    DefaultFrequencySortedDictionary dictionary = new DefaultFrequencySortedDictionary(dictPath, fs1);
    TermStatistics stats = new TermStatistics(new Path(dictPath), fs1);
    /* read query and convert to ids */
    queryTermtoID allQuery = new queryTermtoID(queryFile, dictionary);
    System.out.println("Number of query read: " + allQuery.nq);
    
    /* reading cluster assignments  */
    int [] clusCard = new int [50000];
    int maxClusNo = 0;
    try {
      BufferedReader in = new BufferedReader(new FileReader(args[6]));
      String line;
      while((line = in.readLine()) != null) {
        String [] s = line.split("\\s+");
        int cno = Integer.parseInt(s[1]);
        clusAssign.put(s[0], cno);
        clusCard[cno]++;
        if(maxClusNo < cno)
          maxClusNo = cno;
      }
      in.close();
    }
    catch(IOException e) {
      System.out.println("Exception: reading cluster assignment");
    }
    
    clusStartIndex = new int[maxClusNo + 1];
    clusEndIndex = new int[maxClusNo + 1];
    int sum = 0;
    for(int i = 0; i <= maxClusNo; i++) {
      clusStartIndex[i] = sum;
      clusEndIndex[i] = sum;
      sum += clusCard[i];
    }
    
    /* reading cluster centers */
    double [][] clusCenters = new double [maxClusNo + 1][];
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
      System.out.println("Exception: reading cluster centers");
    }
    
    /* reading word2vec queries */
    int [] qno = new int[10000];
    double [][] queryvec = new double[10000][];
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
      System.out.println("Exception: reading query vectors");
    }
    
    /* determining clusters similar to the query */
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
    
    /* compute ctfs and idfs of query terms */
    for(int k = 0; k < allQuery.nq; k++) {
      List<Float> idf = new ArrayList<Float>();
      List<Long> ctf = new ArrayList<Long>();
      Collections.sort(allQuery.query[k].TermID);
      for(int l = 0; l < allQuery.query[k].TermID.size(); l++) {
        int id = allQuery.query[k].TermID.get(l);
        int df = stats.getDf(id);
        idf.add((float) Math.log((1.0f*numDoc-df+0.5f)/(df+0.5f)));
        ctf.add(stats.getCf(id));
      }
      allQuery.query[k] = new Query(allQuery.query[k].qno, allQuery.query[k].TermID, idf, ctf);
    }
    
    DII = new ComDII[numDoc];
    FileSystem fs = FileSystem.get(new Configuration());
    Path p = new Path(docvecPath);
    if (fs.getFileStatus(p).isDirectory()) {
      numDoc = readSequenceFilesInDir(p, fs, max);
    } else {
      numDoc = readSequenceFile(p, fs, max);
    }
    
    adl = stats.getCollectionSize()/numDocRead;
    
    clusMetaData.clusStartIndex = clusStartIndex;
    clusMetaData.clusEndIndex = clusEndIndex;
    
    Thread = new SSComDIIRankerThread[numThread];
    
    for(int i = 0; i < allScore.length; i++)
      allScore[i] = new Score(-1, -1000.0f, 0);
    
      long startTime = System.nanoTime(); 
  
      for(int j = 0; j < allQuery.nq; j++) {
          clusMetaData.clusRankList = queryDepClus.get(allQuery.query[j].qno);  
          for(int i = 0; i < numThread; i++) {
            Thread[i] = new SSComDIIRankerThread("Thread", clusMetaData, DII, allQuery.query[j], 
                            numTopDoc, i, allScore, adl, numTopClus, numThread);
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
            System.out.print(allScore[l].qid + " Q0 " + DII[allScore[l].docno].key);
            System.out.println(" " + l + " " + allScore[l].score + " bm25-sscomDII"); 
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
        Integer clusId = clusAssign.get(key.toString());
        if(clusId != null) {
          int [] termid = DOC.getTermIds();
          Map<Integer,List<Integer>> map = new HashMap<Integer,List<Integer>>();
          for(int j = 0; j < termid.length; j++) {
            if(map.get(termid[j]) == null) 
              map.put(termid[j], new ArrayList<Integer>());
            map.get(termid[j]).add(j);  
          }
          
          Object [] keys = map.keySet().toArray();
          Arrays.sort(keys);
          int ulen = keys.length;
          int posCounter = 2 * ulen;
          for(int k = 0; k < ulen; k++) {
            List<Integer> temp = map.get(keys[k]);
            diiBuf[k] = (int) keys[k];
            diiBuf[ulen+k] = temp.size();
            int prev = 0;
            for(Integer t : temp) {
              diiBuf[posCounter++] = t-prev;
              prev = t;
            }
          }
          IntWrapper inPos = new IntWrapper(0);
          IntWrapper outPos = new IntWrapper(0);
          codec.compress(diiBuf, inPos, ulen, comDIIBuf, outPos);
          int [] term = Arrays.copyOf(comDIIBuf, outPos.intValue());
          inPos = new IntWrapper(ulen);
          outPos = new IntWrapper(0);
          codec.compress(diiBuf, inPos, ulen, comDIIBuf, outPos);
          int [] freq = Arrays.copyOf(comDIIBuf, outPos.intValue());
          inPos = new IntWrapper(2*ulen);
          outPos = new IntWrapper(0);
          codec.compress(diiBuf, inPos, DOC.getLength(), comDIIBuf, outPos);
          int [] pos = Arrays.copyOf(comDIIBuf, outPos.intValue());
          int c = clusId.intValue();
          
          DII[clusEndIndex[c]++] = new ComDII(term, freq, pos, DOC.getLength(), ulen, key.toString());
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
      FileStatus [] stat = fs.listStatus(path);
      for (int i = 0; i < stat.length; ++i) {
        n += readSequenceFile(stat[i].getPath(), fs ,max);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return n;
  }
}


