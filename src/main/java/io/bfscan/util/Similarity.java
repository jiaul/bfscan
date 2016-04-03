package io.bfscan.util;

public class Similarity {

  static public double cosine(double [] query, double [] clustercenters)
  {
    double sim = 0.0, lenQ = 0.0, lencc= 0.0;
    for(int i = 0; i < query.length; i++) {
      sim += query[i] * clustercenters[i];
      lenQ += query[i] * query[i];
      lencc += clustercenters[i] * clustercenters[i];
    }
    sim = sim/(Math.sqrt(lenQ) * Math.sqrt(lencc));
    return sim;
  }
  
  static public double dotProduct(double [] query, double [] clustercenters)
  {
    double sim = 0.0, lenQ = 0.0, lencc= 0.0;
    for(int i = 0; i < query.length; i++) {
      sim += query[i] * clustercenters[i];
      lenQ += query[i] * query[i];
      lencc += clustercenters[i] * clustercenters[i];
    }
    sim = sim/(Math.sqrt(lenQ) * Math.sqrt(lencc));
    return sim;
  }
  
  static public float distance(float [] cen, float [] doc)
  {
    double sum = 0.0;
    for(int i = 0; i < doc.length; i++)
      sum += (cen[i]-doc[i])*(cen[i]-doc[i]);
    
    return (float) Math.sqrt(sum);
  }
  
}
