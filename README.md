***Compute Graph***

[![Build Status](https://travis-ci.org/canorbal/ComputeGraph.svg?branch=master)](https://travis-ci.org/canorbal/ComputeGraph)


I. Trivia

This library is for calculations on tables.

Table is a sequence of dict-like objects where each 
dict is a row of table and every key in the dicts is a 
column in the table. 

Calculation is a sequence of 
transformations. Computational Graph is a chain of 
transformations where each node is a transform operation.
Definition of operations is separated from its calculations.
    
    
    
    
    
II. API.

All nodes are defined in [keras](https://keras.io) style.

```python
if __name__ == "__main__":

    input_node = Input()
    mapper = Map(split_text)(input_node)
    sort = Sort("word")(mapper)
    reduce = Reduce(word_counter, "word")(sort)

    graph = Graph(input_node=input_node, output_node=reduce)
    graph.run(input_file="data/text_corpus.txt",
              output_file=open("word_count.txt", "w"))

```
    
 
 Operations in the graph work with rows in the table (not with entire table),
 because this approach avoids extra copying.
 
 The list of all possible operations:
 
   1) Map
   
   Apply a mapper operation to every row in table. For example, one
   of possible mapper:
     
```python
def tokenizer_mapper(r):
    """
    splits rows with 'text' field into set of rows with 'token' field
    (one for every occurence of every word in text)
    """
    tokens = r['text'].split()
    for token in tokens:
        yield {
               'doc_id' : r['doc_id'],
               'word' : token,
        }
```
   
   2) Sort
   
   Sort a table by a set of keys.
   
   3) Fold
   
   Makes [convolution](https://en.wikipedia.org/wiki/Fold_(higher-order_function))
    of a table to a single row via binary associated 
   operation. Fold node needs a folder and initial state. Fold calls folder
   on (state, new row) and returns new state. The result of this operation
   is a finish state (after calculations on all rows in series). 
   
   Possible folder:
   
```python
def sum_columns_folder(state, record):
     for column in state:
         state[column] += record[column]
     return state
```
   
   4) Reduce
   
   This operation is similar to Map but it is called for rows which have the same
   keys. These rows are united in a list and then passed to reducer. Reduce 
   works for O(n) if rows are sorted by required keys so input sequence of
   dicts should be sorted.
   
   Example of reducer:
   
```python
def term_frequency_reducer(records):
      '''
      calculates term frequency for every word in doc_id
      '''
      word_count = Counter()
      for r in records:
          word_count[r['word']] += 1
          
      total  = sum(word_count.values)
      for w, count in word_count.items():
          yield {
              'doc_id' : r['doc_id'],
              'word' : w,
              'tf' : count / total
          }
```
   
   This reducer takes a list of rows with the same "doc_id" and count frequncy
   for each word in a document.
   
   5) Join
   
   Works like [SQL Join](https://en.wikipedia.org/wiki/Join_(SQL)) operation.
   The result of one graph is joined to another graph.
   
   Join takes another graph as an argument. [Topological sort](https://en.wikipedia.org/wiki/Topological_sorting)
   is used to find optimal order of computation of different graphs.
   
   
   
   
   
   
