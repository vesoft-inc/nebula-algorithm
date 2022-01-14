# Nebula Algorithm test

## Data preparation

1. Install Nebula Graph 3.0.0
    * cluster info:
    
    `3 metad, 3 graphd, 3 storaged`
    
    * space info:
    
    |Name	    |Partition Number|Replica Factor|Charset|Vid Type|
    |-----------|----------------|--------------|-------|--------|
    |algo_space	|       100	     |     1	    |  uft8	|  int64 |

2. Put LDBC sf100 dataset to HDFS: /ldbc/sf100

    We use Nebula Exchange to import ldbc sf100 dataset into Nebula, importing details see: https://github.com/vesoft-inc/nebula-exchange/tree/master/bench
    

## Test algorithms with HDFS datasource and HDFS data sink
We use LDBC's `forum_hasMember_person` edge dataset to test the algorithms.

 |     algorithms   |   data amount  | concurrency |executor-memory|iter|    other params      | duration|
 |------------------|----------------|-------------|---------------|----|----------------------|---------|
 |  pagerank	    | 180 millions	 |    30 	   |     45G       | 10 |resetProb:0.15        | 1.1min  |
 |  trianglecount   | 180 millions   |    30 	   |     45G       | -  |      -               |  19min  |
 |       lpa        | 180 millions	 |    30 	   |     45G       | 10 |      -               |  11min  |
 |       wcc        | 180 millions	 |    30 	   |     45G       | 10 |      -               |   40s   |
 |       scc   	    | 180 millions	 |    30 	   |     45G       | 10 |      -               |   38s   |
 |clustercoefficient| 180 millions	 |    30 	   |     45G       | -  |      -               |  25min  |
 |      kcore	    | 180 millions	 |    30 	   |     45G       | 10 |   degree:1           |   1min  |
 | degreeCentrality | 180 millions	 |    30 	   |     45G       | -  |      -               |   34s   |
 |    louvain	    | 180 millions	 |    30 	   |     45G       | 10 |internalIter:5,tol:0.5|  10min  |
 |       BFS	    | 180 millions	 |    30 	   |     45G       | 10 |root:17592186046139   |   52s   |
 |      HANP	    | 180 millions	 |    30 	   |     45G       | 10 |hop:0.1,preference:1.0|  17min  |
 |     closeness    | 180 millions	 |    30 	   |     45G       | -  |      -               |  17min  |

# Test algorithms with Nebula datasource and Nebula data sink
We use Nebula's `HAS_MEMBER` edge type to test the algorithms, and the result is writen into new tag.

 |     algorithms   |   data amount  | concurrency |executor-memory|iter|    other params      | duration|
 |------------------|----------------|-------------|---------------|----|----------------------|---------|
 |  pagerank	    | 180 millions	 |    30 	   |     45G       | 10 |resetProb:0.15        |  5.2min |
 |  trianglecount   | 180 millions   |    30 	   |     45G       | -  |      -               |  12min  |
 |       lpa        | 180 millions	 |    30 	   |     45G       | 10 |      -               |  44min  |
 |       wcc        | 180 millions	 |    30 	   |     45G       | 10 |      -               |  1.8min |
 |       scc   	    | 180 millions	 |    30 	   |     45G       | 10 |      -               |  2min   |
 |clustercoefficient| 180 millions	 |    30 	   |     45G       | -  |      -               |  11min  |
 |      kcore	    | 180 millions	 |    30 	   |     45G       | 10 |   degree:1           |  3.2min |
 | degreeCentrality | 180 millions	 |    30 	   |     45G       | -  |      -               |  3.6min |
 |    louvain	    | 180 millions	 |    30 	   |     45G       | 10 |internalIter:5,tol:0.5|  14min  |
 |       BFS	    | 180 millions	 |    30 	   |     45G       | 10 |root:17592186046139   |  2.3min |
 |      HANP	    | 180 millions	 |    30 	   |     45G       | 10 |hop:0.1,preference:1.0|  52min  |
 |     closeness    | 180 millions	 |    30 	   |     45G       | -  |      -               |  42min  |
