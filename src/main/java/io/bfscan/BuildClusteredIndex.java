package io.bfscan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import tl.lin.data.array.IntArrayWritable;
import io.bfscan.data.TermStatistics;

public class BuildClusteredIndex {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private BuildClusteredIndex() {}
  public static  ComKeyValue[] data;
  public static int numDocRead = 0;
  private static IndexerThread [] Thread;
  public static HashMap<String, Integer> clusAssign = new HashMap<String, Integer>();
  public static int [] clusCard = new int [10000];
  public static int [] cumClusCard = new int [10000];
  public static int numOfClus = 0;
  public static int [] clusterBoundIndex =  new int[10000];
  public static void main(String[] args) throws IOException {
    if (args.length < 5) {
      System.out.println("args: [doc. vectors path] [dictionary path] [cluster assignment] [index name] [# thread]");
      System.exit(-1);
    }

    String docvecPath = args[0];
    String dictPath = args[1];
    String clusAssignFile = args[2];
    String indexName = args[3];
    int numOfThread = Integer.parseInt(args[4]);
    
    File indexDir = new File(indexName);
    if(!indexDir.exists())
      indexDir.mkdir();
    
    // load dictionary
    Configuration conf = new Configuration();
    FileSystem fs1 = FileSystem.get(conf); 
    TermStatistics stats = new TermStatistics(new Path(dictPath), fs1);
    int vocabSize = stats.getVocabularySize();
    System.out.println("Vocab size: " + vocabSize);
    
    Arrays.fill(clusCard, 0);
    int numDoc = 0;
    // reading cluster assignments
    try {
      BufferedReader in = new BufferedReader(new FileReader(clusAssignFile));
      String line;
      while((line = in.readLine()) != null) {
        String [] s = line.split("\\s+");
        int cno = Integer.parseInt(s[1]);
        clusAssign.put(s[0], cno);
        clusCard[cno]++;
        if(numOfClus < cno)
          numOfClus = cno;
        numDoc++;
      }
      in.close();
    }
    catch(IOException e) {
      System.out.println("Exception: reading cluster assignments");
    }
    numOfClus += 1;
    
    System.out.println("Reading cluster assignments complete");
    
    data = new ComKeyValue[numDoc];
    
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
      readSequenceFilesInDir(p, fs, max);
    } else {
      readSequenceFile(p, fs, max);
    }
    
    System.out.println("Reading documents complete\n" + "# of doc read: " + numDoc);
    Thread = new IndexerThread[numOfThread];
    int [] doclen = new int[numDoc];
    long startTime = System.nanoTime(); 
    
    int start, end;
    for(int c = 0; c < numOfClus; c += numOfThread) {
      int m = Math.min(numOfThread, numOfClus-c);
      for(int i = 0; i < m; i++) {
        String name = "thread-" + i;
        if((c+i) == 0) 
           start = 0;
        else 
           start = cumClusCard[c+i-1];
        end = clusterBoundIndex[c+i];
        Thread[i] = new IndexerThread(name, data, start, end, c+i, vocabSize, indexName, doclen);
        Thread[i].start();
      }
      
      try {
        for(int i = 0; i < m; i++)
          Thread[i].Thr.join();
        }
      catch (InterruptedException e) {
          System.out.println("Main thread Interrupted");
      }
    }
    
    
    ObjectOutputStream statStream = null;
    ObjectOutputStream docidStream = null;
    try {
      statStream = new ObjectOutputStream(new FileOutputStream(indexName + "/stat"));
      statStream.writeInt(numDoc);
      statStream.writeInt(numOfClus);
      docidStream = new ObjectOutputStream(new FileOutputStream(indexName + "/docid"));
      for(int i = 0; i < numDoc; i++) {
        if(data[i] != null) {
          docidStream.writeObject(i);
          docidStream.writeObject(data[i].key);
          docidStream.writeObject(doclen[i]);
        }
      }
    }
    catch (IOException ex) {
      ex.printStackTrace();
    }
    
    try {
      statStream.close();
      docidStream.close();
    }
    catch (IOException ex) {
      ex.printStackTrace();
    }
    
    long endTime = System.nanoTime();
    System.out.println("Time: " + (endTime-startTime)/1000000000.0 + " sec.");
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
        c = clusAssign.get(key.toString());
        if(c != null) {
          int i = c.intValue();
          data[clusterBoundIndex[i]++] = new ComKeyValue(key.toString(), value);
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


