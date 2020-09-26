from pyspark import SparkContext    
import json
import csv
import sys

if __name__ == "__main__":

    business_file_path = "./Data/business.json"
    review_file_path = "./Data/review.json"
    output_file_path = "./Data/user_business.csv"
    state = "NV"

    sc = SparkContext.getOrCreate()
    business_list = sc.textFile(business_file_path).map(lambda row: json.loads(row))\
        .map(lambda json_data: (json_data["business_id"], json_data["state"]))\
            .filter(lambda kv: kv[1] == state).map(lambda line: line[0]).collect()
    
    user_list = sc.textFile(review_file_path).map(lambda row: json.loads(row))\
        .map(lambda json_data: (json_data["user_id"],json_data["business_id"]))\
            .filter(lambda kv: kv[1] in business_list).collect()
    
    with open(output_file_path, mode= 'w', newline='') as result_file:
        #write output to csv file
        result_writer = csv.writer(result_file)
        result_writer.writerow(["user_id","business_id"])
        result_writer.writerows(i for i in user_list)
        result_file.close()