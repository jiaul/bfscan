package io.bfscan;

public class Score {

  public String docid;
  public float score;
  public int qid;
  public int docno;
  
  public Score(String docid, float score, int qid) 
  {
    this.docid = docid;
    this.score = score;
    this.qid = qid;
  }
  
  public Score(int docno, float score, int qid) 
  {
    this.docno = docno;
    this.score = score;
    this.qid = qid;
  }
  
}