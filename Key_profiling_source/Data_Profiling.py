'''
Created on Sep 22, 2014

@author: Jeeva
'''
#importing all the required packages
import sys
import time
from multiprocessing import Pool
from itertools import chain
from functools import partial
import itertools
import csv

#Function to create a dictionary and loading each possible world as one key and all its data as values in it.
def tokenize(filename):
    dicchunk = {} 
    with open(filename) as f:
        f_csv = csv.reader(f, delimiter='\t')
        headers = next(f_csv)
        attr_string = list(x for x in headers if x != 'Id')
        l = []
        for i in range(0,len(attr_string)):
            l.append(1*i)
        attributes = dict(zip(l,attr_string))
        dataset = tuple(tuple(x) for x in f_csv)
        for tup in dataset:
            dicchunk.setdefault(tup[-1],[])
            dicchunk[tup[-1]].append(tup[:-1])
    return(attributes,dicchunk)

#function to create a dictionary for probability for each world
def probability(filename):
    with open(filename) as f:
        f_csv = csv.reader(f, delimiter='\t')
        headers = next(f_csv)
        prob = {}
        for row in f_csv:
            k, v = row
            prob[k] = float(v)
    return(headers,prob)

#key mining algorithm implementation for finding possible keys from each world
def keymining(values):
    data1 = itertools.combinations(values,2)
    new = ((i for i, t in enumerate(zip(*pair)) if t[0]!=t[1]) for pair in data1)
    skt = set(frozenset(temp) for temp in new)
    newset = set(s for s in skt if not any(p < s for p in skt))

    empty = frozenset(" ")
    tr_x = set(frozenset(i) for i in empty)
    tr = set(frozenset(i) for i in empty)
    for e in newset:
        tr.clear()
        tr = tr.union(tr_x)
        tr_x.clear()
        for x in tr:
            for a in e:
                if x == empty:
                    tmp = frozenset(frozenset([a]))
                    tr_x = tr_x.union([tmp])
                else :
                    tmp = frozenset(frozenset([a]).union(x))
                    tr_x = tr_x.union([tmp])
        tr.clear()
        tr = tr.union(tr_x)
        tr = set(l for l in tr if not any(m < l for m in tr))
    return tr

#Function to call key mining function in parallel based on the number of worlds
def main():
    p = Pool(len(data)) #number of processes = number of CPUs
    keys, values= zip(*data.items()) #ordered keys and values
    processed_values= p.map( keymining,values )
    map_output= dict( zip(keys, processed_values ) )
    p.close() # no more tasks
    p.join()  # wrap up current tasks
    return map_output

#Function to make combinations of attributes
def subsets(attributes,n): # the argument n is the number of items to select
    res = tuple(itertools.combinations(attributes, n)) # create a list from the iterator
    return res

#Function to parallelize the combination generation and convert the output to dictionary
def comb(numbers):
    p = Pool(4)
    times = range(0, len(attributes)+1)     
    func1 = partial(subsets,attributes)                                        
    values = p.map(func1, times) # pass the range as the sequence of arguments!
    p.close()
    p.join()
    sets = chain.from_iterable(values)
    frozensets = map(frozenset, sets)
    comb_values = dict.fromkeys(frozensets, 0)
    return comb_values

#Function to do reduce step
def reduce(prob,map_output,comb_value_chunk):
    key_values = dict(item for item in comb_value_chunk)  # Convert back to a dict
    for k in key_values.keys():
        for ky,values in map_output.items():
            for l in values:
                if(k >= l):
                    key_values[k] += prob[ky]
                    break
    return key_values

#Function to make chunk and parallelize the reduce step, then making a single dictionary for final output
def parallel_reducer(prob,map_output,comb_values):
    p = Pool(5) #number of processes = number of CPUs
    # Break the output dict into 4 lists of (key, value) pairs
    items = tuple(comb_values.items())
    chunksize = 50
    chunks = (items[i:i + chunksize ] for i in range(0, len(items), chunksize))
    func2 = partial(reduce, prob, map_output)
    reduced_key_values = p.map(func2,chunks)
    p.close() # no more tasks
    p.join()  # wrap up current tasks
    final_reducedvalues = {}
    for d in reduced_key_values:
        final_reducedvalues.update(d)
    return final_reducedvalues

#Function to convert integer attributes to string and write it in a file
def convert(final_reducedvalues,attributes,output_file):
    out = {
    frozenset(attributes[key] for key in keys): value
    for keys, value
    in final_reducedvalues.items()
    }
    with open (output_file, 'w') as fp:
        for p in out.items():
            fp.write("%s:%s\n" % p)


if __name__ == '__main__':
    start_time = time.time()
    attributes,data = tokenize(sys.argv[1]) #Input dataset
    headers,prob = probability(sys.argv[2]) #Probability relation
    output_path = sys.argv[3] #output path
    map_output = main()
    comb_values = comb(attributes)
    final_reducedvalues = parallel_reducer(prob,map_output,comb_values)
    print(time.time() - start_time)
    output = convert(final_reducedvalues,attributes,output_path)
