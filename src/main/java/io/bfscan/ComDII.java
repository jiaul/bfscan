package io.bfscan;

public class ComDII {
  int [] term;
  int [] freq;
  int [] pos;
  int len;
  int ulen;
  String key;
  
  public ComDII(int [] term, int [] freq, int [] pos, int len, int ulen, String key) {
    this.term = term;
    this.freq = freq;
    this.pos = pos;
    this.len = len;
    this.ulen = ulen;
    this.key = key;
  }
  
}
