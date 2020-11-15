import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    '''
    Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
    print the sum of those numbers to console.
    Each row of the input file contains 10 prime numbers separated by spaces.
    '''
    conf = SparkConf().setAppName("sum_of_numbers").setMaster("local[*]")
    sc = SparkContext(conf = conf)
   
    lines = sc.textFile("in/prime_nums.text")
    numbers = lines.flatMap(lambda line: line.split("\t"))

    valid_numbers = numbers.map(lambda number: number)

    valid = valid_numbers.map(lambda number: int(number))

    print(valid.reduce(lambda x, y: x+y))
