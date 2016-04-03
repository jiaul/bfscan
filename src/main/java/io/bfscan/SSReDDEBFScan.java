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

import io.bfscan.util.tuple;

public class SSReDDEBFScan {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private SSReDDEBFScan() {}
  public static  DecomKeyValue[] data;
  public static int numDocRead = 0;
  public static int numFile = 0;
  private static final PForDocVector DOC = new PForDocVector();
  private static SSRankerThread [] Thread;
  public static Score [] allScore;
  public static HashMap<String, Integer> clusAssign = new HashMap<String, Integer>();
  public static int [] clusCard = new int [10000];
  public static int [] cumClusCard = new int [10000];
  public static int numOfClus = 0;
  public static int [] clusterBoundIndex =  new int[10000];
  public static int [] csiDocs;
  public static int numCsiDoc;
  public static int [] rankOfClusters = new int[10000];
  public static csiScore csiDocScores [];
  public static clusterInfo cluster = new clusterInfo();
  public static int [] csiDocClustID;
  public static int [] [] latency = new int[48][1000];
  public static int reddeCost = 0;
  public static void main(String[] args) throws IOException {
    if (args.length < 7) {
      System.out.println("args: [doc. vectors path] [dictionary path] [# thread] [query file]");
      System.out.println("[# doc in collection] [cluster assignment] [sample size (%)]");
      System.exit(-1);
    }

    String docvecPath = args[0];
    String dictPath = args[1];
    int numThread = Integer.parseInt(args[2]);
    String queryFile = args[3];
    int numDoc = Integer.parseInt(args[4]);
    String clusAssignPath = args[5];
    float sampleSize = Float.parseFloat(args[6]);
    
    int numTopDoc = 1000;
    allScore = new Score[numTopDoc * numThread];
    data = new DecomKeyValue[numDoc];
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
      BufferedReader in = new BufferedReader(new FileReader(clusAssignPath));
      String line;
      while((line = in.readLine()) != null) {
        String [] s = line.split("\\s+");
        int cno = Integer.parseInt(s[1]);
        clusAssign.put(s[0], cno);
        clusCard[cno]++;
        if(cno > numOfClus)
          numOfClus = cno;
      }
      in.close();
    }
    catch(IOException e) {
      System.out.println("Exception reading cluster assignments");
    }
    numOfClus += 1;
    
    int maxCsiSize = (numDoc * 10)/100;
    csiDocs = new int[maxCsiSize];
    csiDocScores = new csiScore[maxCsiSize];
    for(int i = 0; i < maxCsiSize; i++) 
      csiDocScores[i] =  new csiScore();
    csiDocClustID = new int[maxCsiSize];
    
    Arrays.fill(cumClusCard, 0);
    cumClusCard[0] = clusCard[0];
    clusterBoundIndex[0] = 0;
    for(int i = 1; i < numOfClus; i++) {
      cumClusCard[i] = clusCard[i] + cumClusCard[i-1];
      clusterBoundIndex[i] = clusCard[i-1] + clusterBoundIndex[i-1];
    }  
    
    int max = Integer.MAX_VALUE;

    FileSystem fs = FileSystem.get(new Configuration());
    Path p = new Path(docvecPath);

    if (fs.getFileStatus(p).isDirectory()) {
      numDoc = readSequenceFilesInDir(p, fs, max);
    } else {
      numDoc = readSequenceFile(p, fs, max);
    }
    float adl = stats.getCollectionSize()/numDocRead;
    
    // compute ctfs and idfs of query terms
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
    
    cluster.cumClusCard = cumClusCard;
    cluster.numOfThread = numThread;
    cluster.clusCard = clusCard;
    cluster.clusterBoundIndex = clusterBoundIndex;
    
    Thread = new SSRankerThread[numThread];
    int [] cost = new int[numThread];
    
    makeCSIDocs(sampleSize); // select CSI documents
    // initialize score array 
    for(int i = 0; i < allScore.length; i++)
      allScore[i] = new Score("nodoc", -1.0f, 0);
    
    HashMap<Integer, int []> queryDepClus = new HashMap<Integer, int []>();
    for(int i = 0; i < allQuery.nq; i++) {
      queryDepClus.put(allQuery.query[i].qno, ReDDE(allQuery.query[i]));
      if((i+1)%99 == 0) {
        System.out.println("ReDDE Cost: " + reddeCost);
        reddeCost = 0;
      }
    }  
    
    int [] clusCut = {1, 2, 3, 4, 5, 10, 15, 20};
    for(int k = 0; k < clusCut.length; k++) {
        long startTime = System.nanoTime(); 
        long clusterSizeSum = 0;
        long latencySum = 0;
        Arrays.fill(cost, 0);
        for(int j = 0; j < allQuery.nq; j++) {
            for(int [] row : latency)
              Arrays.fill(row, 0);
            cluster.clusters = queryDepClus.get(allQuery.query[j].qno);
            cluster.numOfTopClus = Math.min(clusCut[k], cluster.clusters.length);
            for(int m = 0; m < cluster.numOfTopClus; m++)
              clusterSizeSum += cluster.clusCard[cluster.clusters[m]];
            for(int i = 0; i < numThread; i++) {
              Thread[i] = new SSRankerThread("Thread", cluster, data, allQuery.query[j], numTopDoc, i, allScore, adl, 
                                             cost, latency);
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
              System.out.println(allScore[l].qid+" Q0 "+allScore[l].docid+" "+l+" "+allScore[l].score+" ss-bm25-redde-"+ clusCut[k]); 
              allScore[l].score = -1.0f;
            }
            
            int maxsofar = -1;
            for(int i1 = 0; i1 < cluster.numOfTopClus; i1++) {
              int tsum = 0;
              for(int j1 = 0; j1 < numThread; j1++)
                tsum += latency[j1][i1];
              if(maxsofar < tsum)
                maxsofar = tsum;
            }
            latencySum += maxsofar;
            
            long costSum = 0;
            for(int l = 0; l < numThread; l++)
              costSum += cost[l];
            Arrays.fill(cost, 0);
            int clusSizeSum = 0;
            for(int m = 0; m < cluster.numOfTopClus; m++)
              clusSizeSum += cluster.clusCard[cluster.clusters[m]];
            System.out.print("Cost: " + allQuery.query[j].qno + " #cluster: "+clusCut[k]+"\t"+costSum+"\t"+maxsofar);
            System.out.println("\t"+(100.0*clusSizeSum)/numDocRead);
            
        }
        
        long endTime = System.nanoTime(); 
        System.out.println("Time: " + (endTime-startTime)/1000000000.0+" s | # of query: "+allQuery.nq+" # cluster: " + clusCut[k]);
        
        /*double percOfDoc = (100.0f * clusterSizeSum/allQuery.nq)/numDocRead;
        long costSum = 0;
        for(int l = 0; l < numThread; l++)
          costSum += cost[l];
        System.out.println("Cost: # cluster: "+clusCut[k]+"\t"+costSum/100+"\t"+latencySum/100+"\t"+percOfDoc);*/
        
   }
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
          data[clusterBoundIndex[i]++] = new DecomKeyValue(key.toString(), DOC.getTermIds());
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
  
  private static void makeCSIDocs(float p)
  {
    numCsiDoc = 0;
    Random rand = new Random();
    for(int j = 0; j < numOfClus; j++) {
      int n = (int) ((clusCard[j]*p)/100);
      for(int c = 0; c < n; c++) {
        if(j > 0)
          csiDocs[numCsiDoc] = cumClusCard[j-1] + rand.nextInt(cumClusCard[j]-cumClusCard[j-1]);
        else
          csiDocs[numCsiDoc] = rand.nextInt(cumClusCard[0]); 
        csiDocClustID[numCsiDoc] = j;
        numCsiDoc++;
      }
    }
  }
  
  
  private static int [] ReDDE(Query query)
  {
      int qlen = query.TermID.size();
      double [] idf = new double[qlen];
      int [] qtid = new int[qlen];
      int [] tf = new int[qlen];
      
      float k1 = 1.0f;
      float b = 0.5f;
      float adl = 450.0f;
      
      for(int i = 0; i < qlen; i++) {
        idf[i] = query.idf.get(i);
        qtid[i] = query.TermID.get(i);
      }
      for(int i = 0; i < numCsiDoc; i++) {
        Arrays.fill(tf, 0);
        for(int termid : data[csiDocs[i]].doc)
          for(int j = 0; j < qlen; j++)
            if(termid == qtid[j])
              tf[j]++;
        double score = 0.0;
        double dlen = data[csiDocs[i]].doc.length;
        for(int j = 0; j < qlen; j++)
          score += idf[j] * ((k1+1.0f) * tf[j])/(k1*(1.0f-b+b*dlen/adl)+tf[j]);
        csiDocScores[i].score = score;
        csiDocScores[i].clustId = csiDocClustID[i];
        
        if(score > 0.0f)
          reddeCost++;
      }  
      
      Arrays.sort(csiDocScores, new Comparator<csiScore>() {
        public int compare(csiScore a, csiScore b) {
          if(a.score > b.score)
            return -1;
          else if(a.score < b.score)
            return 1;
          else
            return 0;
       }
     });
     
     HashMap <Integer, Integer> map = new HashMap<Integer, Integer>();
     int cut = Math.min(1000, numCsiDoc);
     for(int i = 0; i < cut && csiDocScores[i].score > 0.0f; i++) {
        Integer t = map.get(csiDocScores[i].clustId);
        if(t == null)
          map.put(csiDocScores[i].clustId, 1);
        else
          map.put(csiDocScores[i].clustId, t.intValue()+1);
     }   
     tuple [] sel = new tuple[map.keySet().size()];
     int c = 0;
     for(int t : map.keySet()) {
       sel[c++] = new tuple(t, (1.0f * map.get(t) * clusCard[t])/numCsiDoc);
     }
     
     Arrays.sort(sel, new Comparator<tuple>() {
       public int compare(tuple a, tuple b) {
         if(a.y > b.y)
           return -1;
         else if(a.y < b.y)
           return 1;
         else
           return 0;
       }
     });
    
    int [] clusters = new int[sel.length];
    for(int i = 0; i < clusters.length; i++)
      clusters[i] = sel[i].x;
    
    return clusters; 
  }
  
}


