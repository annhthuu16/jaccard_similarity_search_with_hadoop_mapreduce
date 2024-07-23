### Introduction
In the era of big data, similarity search is a crucial operation spanning various interdisciplinary research fields such as information retrieval, clustering, data mining, and machine learning, as well as practical applications like duplicate detection, classification, social network analysis, and e-commerce. Similarity search plays a vital role in addressing numerous issues in natural sciences, engineering, and biomedical fields, and it also has significant implications in social psychology.  
MapReduce, a parallel programming model, has emerged as a promising solution for large-scale data processing. It enables specialized computations on a cluster of commodity machines with high fault tolerance. 

### Approach
#### Instant Approaches and Jaccard-based method
Handling large datasets for similarity searches can be inefficient due to redundant data and high computational costs. Instant approaches use the MapReduce paradigm and inverted indices to focus on relevant data, enhancing performance. By filtering out irrelevant data based on the given query, these methods reduce the computational load and improve efficiency, making them well-suited for large-scale data analysis.  
The Jaccard Algorithm measures similarity between finite sample sets and is calculated by the size of the intersection divided by the size of the union of the sample sets. 

\[
J(A, B) = \frac{|A \cap B|}{|A \cup B|} = \frac{|A \cap B|}{|A| + |B| - |A \cap B|}
\]

The Jaccard coefficient varies between 0 and 1 (0 <= Jaccard coefficient <= 1). When the coefficient is equal to 0, there is no similarity between the two sets and vice versa.

#### MapReduce jobs from Jaccard-based method
This section explains how the Jaccard-based method reduces the number of MapReduce jobs required for similarity search, thereby improving overall performance.  
The implementation consists of two main phases, each handled by a separate MapReduce job.

(https://github.com/user-attachments/assets/a2867732-26d9-4891-8716-d112af4d7b45)  
Overview of MapReduce-1: The process of building the customized inverted index.

#### Phase 1: (MapReduce-1): Building the Customized Inverted Index 
The objective of this phase is to construct a customized inverted index from the dataset and a given query object. This phase includes the following steps:
- **Duplicate Term Filtering**: Remove duplicate terms from the dataset.
- **Common Term Filtering**: Discard common terms that appear frequently across the dataset.
- **Lonely Term Filtering**: Eliminate terms that appear in only one document (lonely terms).

![Inverted Index Construction](https://github.com/user-attachments/assets/50e88be9-3582-4e20-bacf-38f7acaa843d)  
*Building the inverted index for the Jaccard-based method with a given query object (Phan et al., 2014).*

The filtering processes ensure that only relevant and meaningful terms are included in the inverted index. The resulting index pairs each term with the documents in which it appears, sorted by their relevance scores.

#### Phase 2 : (MapReduce-2): Computing Similarity 
This phase utilizes the customized inverted index to compute similarity scores between the query object and the documents in the dataset. It involves:
- **Pre-pruning**: Filtering out objects that are unlikely to match the query based on predefined criteria.
- **Range Query Filtering**: Refining the results to include only those within a specified similarity range.
- **k-NN Query Filtering**: Identifying the k-nearest neighbors to the query object based on similarity scores.

#### MapReduce Jobs Overview
- **MAP-1 Task**: This task parses terms from the input documents and emits intermediate key-value pairs, where URLi is the document identifier and Wi is the term weight.
- **REDUCE-1**: The reducer processes the intermediate key-value pairs to build a customized inverted index. The output is a key-value pair, where documents are sorted in descending order by their term weights Wi.
- **MAP-2**: This task processes the key-value pairs from the REDUCE-1 task and emits candidate pairs.
- **REDUCE-2**: The final reducer aggregates the candidate pairs and computes their similarity scores, producing the final similarity ranking.

![MapReduce-2 Overview](https://github.com/user-attachments/assets/e59ce7d3-f087-4153-887d-c74f413b6b06)  
*Overview of MapReduce-2: Computing similarity scores with the customized inverted index (Phan et al., 2014).*
