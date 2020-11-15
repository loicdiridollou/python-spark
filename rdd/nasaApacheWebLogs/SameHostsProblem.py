from pyspark import SparkContext, SparkConf

def is_not_header(line: str):
    return not (line.startswith("hosts") and "bytes" in line)

def splitSpace(line: str):
    splits = line.split(" ")
    return "{}".format(splits[0])


if __name__ == "__main__":
    '''
    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
    "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
    Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
    Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

    Example output:
    vagrant.vf.mmc.com
    www-a1.proxy.aol.com
    .....    

    Keep in mind, that the original log files contains the following header lines.
    host    logname    time    method    url    response    bytes

    Make sure the head lines are removed in the resulting RDD.
    '''
    conf = SparkConf().setAppName("same_hosts").setMaster("local[1]")
    sc = SparkContext(conf = conf)

    july_first_logs = sc.textFile("in/nasa_19950701.tsv")
    august_first_logs = sc.textFile("in/nasa_19950801.tsv")

    july_first_hosts = july_first_logs.map(lambda line: line.split("\t")[0])
    august_first_hosts = august_first_logs.map(lambda line: line.split("\t")[0])

    intersection = july_first_hosts.intersection(august_first_hosts)

    clean_hosts = intersection.filter(lambda host: host != "host") 
 
    clean_hosts.saveAsTextFile("out/nasa_logs_same_host.csv")