package io.bfscan;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

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

public class BuildSplitIndex {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private BuildSplitIndex() {}
  public static  ComKeyValue[] data;
  public static int numDocRead = 0;
  private static IndexerThread [] Thread;
  public static void main(String[] args) throws IOException {
    if (args.length < 6) {
      System.out.println("args: [doc. vectors path] [dictionary path] [# split] [index name] [# thread] [# doc]");
      System.exit(-1);
    }

    String docvecPath = args[0];
    String dictPath = args[1];
    int numSplit = Integer.parseInt(args[2]);
    String indexName = args[3];
    int numOfThread = Integer.parseInt(args[4]);
    int numDoc = Integer.parseInt(args[5]);
    File indexDir = new File(indexName);
    if(!indexDir.exists())
      indexDir.mkdir();
    
    // load dictionary
    Configuration conf = new Configuration();
    FileSystem fs1 = FileSystem.get(conf); 
    //DefaultFrequencySortedDictionary dictionary = new DefaultFrequencySortedDictionary(dictPath, fs1);
    TermStatistics stats = new TermStatistics(new Path(dictPath), fs1);
    int vocabSize = stats.getVocabularySize();
    System.out.println("Vocab size: " + vocabSize);
    
    int max = Integer.MAX_VALUE;

    FileSystem fs = FileSystem.get(new Configuration());
    Path p = new Path(docvecPath);
    data = new ComKeyValue[numDoc];
    if (fs.getFileStatus(p).isDirectory()) {
      readSequenceFilesInDir(p, fs, max);
    } else {
      readSequenceFile(p, fs, max);
    }
    
    System.out.println("Reading documents complete\n" + "# of doc read: " + numDocRead);
    Thread = new IndexerThread[numOfThread];
    int [] doclen = new int[numDocRead];
    long startTime = System.nanoTime(); 
    
    int start, end;
    int segSize = numDocRead/numSplit;
    for(int c = 0; c < numSplit; c += numOfThread) {
      int m = Math.min(numOfThread, numSplit-c);
      for(int i = 0; i < m; i++) {
        String name = "thread-" + i;
        start = (c+i) * segSize;
        end = Math.min((c+i+1)*segSize, numDocRead);
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
      statStream.writeInt(numDocRead);
      statStream.writeInt(numSplit);
      docidStream = new ObjectOutputStream(new FileOutputStream(indexName + "/docid"));
      for(int i = 0; i < numDocRead; i++) {
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
        data[numDocRead++] = new ComKeyValue(key.toString(), value);
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


