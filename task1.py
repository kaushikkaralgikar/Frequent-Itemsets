from pyspark import SparkContext
import csv
import math
import copy
import collections
from itertools import combinations
from operator import add
import time
import sys

def get_higher_order_candidate_list(combination_list):
    higher_order_list = list()
    if combination_list is not None and len(combination_list)>0:
        curr_size = len(combination_list[0])
        for i, p1 in enumerate(combination_list[:-1]):
            for p2 in combination_list[i+1:]:
                if(p1[:-1]==p2[:-1]):
                    new_combination = tuple(sorted(list(set(p1).union(set(p2)))))
                    higher_order_list.append(new_combination)
                else:
                    break
        return higher_order_list

def hash_function(pair, bucket_size):
    sum =0
    for i in list(pair):
        sum += int(i)
    return sum % bucket_size

def get_candidate_itemsets(partition, support, data_size, bucket_size):

    baskets = copy.deepcopy(list(partition))
    ps_support = math.ceil(support*len(list(baskets))/data_size)
    baskets_list = list(baskets)

    #first pass to get singletons and bitmap

    bitmap = [0]*bucket_size

    temp_counter = {}
    for basket in baskets_list:
        for item in basket:
            if item not in temp_counter.keys():
                temp_counter[item]=1
            else:
                new_count = temp_counter.get(item)+1
                temp_counter.update({item:new_count})

        for pair in combinations(basket, 2):
            bucket_number = hash_function(pair, bucket_size)
            bitmap[bucket_number] = (bitmap[bucket_number] + 1)

    singleton_list = []
    for k,v in temp_counter.items():
        if temp_counter[k]>= ps_support:
            singleton_list.append(k)   
    singleton_list = sorted(singleton_list)
    bitmap = list(map(lambda value: True if value >= ps_support else False, bitmap))

    candidate_itemset_dict = {}
    counter = 1
    candidate_itemset_dict[str(counter)] = [tuple(i.split(",")) for i in singleton_list]
    candidate_list = singleton_list

    while None is not candidate_list and len(candidate_list)>0:
        counter+=1
        temp_dict = {}
        for basket in baskets_list:
            basket = sorted(list(set(basket).intersection(set(singleton_list))))
            if len(basket)>=counter:
                if counter==2:
                    for pair in combinations(basket, 2):
                        if bitmap[hash_function(pair,bucket_size)]:
                            if pair not in temp_dict.keys():
                                temp_dict[pair] = 1
                            else:
                                temp_dict[pair]+=1
                else:
                    for i in candidate_list:
                        if set(i).issubset(set(basket)):
                            if i not in temp_dict.keys():
                                temp_dict[i] = 1
                            else:
                                temp_dict[i]+=1
        
    
        filtered_dict = dict(filter(lambda j: j[1] >= ps_support, temp_dict.items()))
        candidate_list = get_higher_order_candidate_list(sorted(list(filtered_dict.keys())))
        if len(filtered_dict) == 0:
            break
        candidate_itemset_dict[str(counter)] = list(filtered_dict.keys())
        
    return candidate_itemset_dict.values()

def get_frequent_itemsets(partition, candidate_itemsets):

    temp_dict = {}
    for combination in candidate_itemsets:
        if set(combination).issubset(set(partition)):
            if combination not in temp_dict.keys():
                temp_dict[combination] = 1
            else:
                temp_dict[combination]+=1
    
    yield [tuple((k,v)) for k,v in temp_dict.items()]

def reformat(itemset):
    result = ""
    index = 1
    for combination in itemset:
        if len(combination) == 1:
            result += str("("+str(combination)[1:-2]+"),")
        elif len(combination) != index:
            result = result[:-1]
            result += '\n\n'
            result += (str(combination)+",")
            index = len(combination)
        else:
            result += (str(combination)+",")
    return result[:-1]

if __name__ == "__main__":
    start_time = time.time()
    """
    input_file_path = "./Data/small1.csv"
    output_file_path = "./Data/output_task1_1_4_small1.txt"
    case = "1"
    support = "4"
    """
    input_file_path = sys.argv[3]
    output_file_path = sys.argv[4]
    case = sys.argv[1]
    support = sys.argv[2]

    bucket_size = 0

    sc = SparkContext.getOrCreate()
    data_rdd = sc.textFile(input_file_path).filter(lambda line: line.split(",")[0]!="user_id")
    
    if int(case) == 1:
        basket_rdd = data_rdd.map(lambda line : (line.split(",")[0], line.split(",")[1]))\
            .groupByKey().map(lambda items: (items[0], sorted(list(set(list(items[1]))))))\
                .map(lambda itemsets: itemsets[1])

        bucket_size =121

    elif int(case) == 2:
        basket_rdd = data_rdd.map(lambda line : (line.split(",")[1], line.split(",")[0]))\
            .groupByKey().map(lambda items: (items[0], sorted(list(set(list(items[1]))))))\
                .map(lambda itemsets: itemsets[1])
        
        bucket_size = 49

    data_size = basket_rdd.count()

    candidate_itemset = basket_rdd.mapPartitions(
        lambda partition: get_candidate_itemsets(partition, int(support), data_size, bucket_size)
    ).flatMap(lambda pairs: pairs).distinct() \
        .sortBy(lambda pairs: (len(pairs), pairs)).collect()
        
    frequent_itemsets = basket_rdd.flatMap(
        lambda partition : get_frequent_itemsets(partition, candidate_itemset)
    ).flatMap(lambda pairs:pairs).reduceByKey(add)\
        .filter(lambda pairs: pairs[1]>=int(support))\
            .map(lambda pairs:pairs[0])\
               .sortBy(lambda pairs:(len(pairs), pairs)).collect()
    
    candidate_itemset_str = reformat(candidate_itemset)
    frequent_itemsets_str = reformat(frequent_itemsets)
    
    with open(output_file_path, 'w+') as output_file:
        str_result = 'Candidates:\n' + candidate_itemset_str + '\n\n' \
                     + 'Frequent Itemsets:\n' + frequent_itemsets_str
        output_file.write(str_result)
        output_file.close()

    print("Duration:",time.time()-start_time)