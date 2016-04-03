package io.bfscan;

import java.util.*;
import io.bfscan.data.*;
import java.io.*;
import me.lemire.integercompression.*;
public class IndexerThread implements Runnable {
  public Thread Thr;
  private String name;
  private int start, end, thid;
  private int vocabSize;
  private String indexName;
  private ComKeyValue[] data;
  private int [] doclen;
  private final MTPForDocVector doc = new MTPForDocVector();
  private final IntegerCODEC codec =  new Composition(new FastPFOR(), new VariableByte()); 
  
  public IndexerThread(String name, ComKeyValue[] data, int start, int end, int thid, 
         int vocabSize, String indexName, int [] doclen) {
      this.name = name;
      this.start = start;
      this.end = end;
      this.data = data;
      this.thid = thid;
      this.doclen = doclen;
      this.indexName = indexName;
      this.vocabSize = vocabSize + 1;
  }
  
  public void run() {
    int [] postingLen = new int[vocabSize];
    int [] comPostingLen = new int[vocabSize];  
    int [][] postings = new int[vocabSize][];
    int [] lastDoc = new int[vocabSize];
    int maxPostingLength = 0;
    System.out.println("Starting to process cluster: " + thid + "\t" + start + "\t" + end);
    Arrays.fill(lastDoc, -1);
    /* determine the size of posting lists */
    for(int i = start; i < end; i++) {
      doc.fromIntArrayWritable(data[i].doc, doc);
      doclen[i] = doc.getLength();
      for(int termid : doc.getTermIds()) {
        if(lastDoc[termid] == i)
          postingLen[termid]++;
        else {
          postingLen[termid] += 3;
          lastDoc[termid] = i;
        }
        if(maxPostingLength < postingLen[termid])
          maxPostingLength = postingLen[termid];
      }
    }
   
    // allocate memory for posting lists
    for(int i = 0; i < vocabSize; i++)
      postings[i] = new int[postingLen[i]];
    System.out.println("Memory allocated for posting lists for cluster: " + thid);
    
    Arrays.fill(postingLen, 0);  
    Arrays.fill(lastDoc,  0);
    Arrays.fill(lastDoc, 0);
    
    for(int i = start; i < end; i++) {
      doc.fromIntArrayWritable(data[i].doc, doc);
      int [] termid = doc.getTermIds();
      Map<Integer,List<Integer>> map = new HashMap<Integer,List<Integer>>();
      for(int j = 0; j < termid.length; j++) {
        if(map.get(termid[j]) == null) 
          map.put(termid[j], new ArrayList<Integer>());
        map.get(termid[j]).add(j);  
      }
      
      for(int key : map.keySet()) {
        postings[key][postingLen[key]++] = i - lastDoc[key];
        lastDoc[key] = i;
        List<Integer> temp = map.get(key);
        postings[key][postingLen[key]++] = temp.size();
        int prevPos = 0;
        for(Integer p : temp) {
          postings[key][postingLen[key]++] = p - prevPos;
          prevPos = p;
        }
      }
    }
    
    int [] compressedData = new int[maxPostingLength];
    String postingFileName = indexName + "/posting." + thid;
    String ofsetFileName = indexName + "/posting.meta." + thid;
    ObjectOutputStream postingStream = null;
    ObjectOutputStream ofsetStream = null;
    try {
      postingStream = new ObjectOutputStream(new FileOutputStream(postingFileName));
      ofsetStream = new ObjectOutputStream(new FileOutputStream(ofsetFileName));
      for(int i = 0; i < vocabSize; i++) {
        IntWrapper inPos = new IntWrapper(0);
        IntWrapper outPos = new IntWrapper(0);
        codec.compress(postings[i], inPos, postingLen[i], compressedData, outPos);
        int comSize = outPos.intValue();
        for(int j = 0; j < comSize; j++)
          postingStream.writeInt(compressedData[j]);
        comPostingLen[i] = comSize;
      }
      ofsetStream.writeObject(comPostingLen);
      ofsetStream.writeObject(postingLen);
    }
    catch (IOException ex) {
      ex.printStackTrace();
    }
    
    try {
      postingStream.close();
      ofsetStream.close();
    }
    catch(IOException ex) {
      ex.printStackTrace();
    }
    
  }
  
  public void start () {
     if (Thr == null) {
        Thr = new Thread (this, name);
        Thr.start ();
     }
  }
  
}
