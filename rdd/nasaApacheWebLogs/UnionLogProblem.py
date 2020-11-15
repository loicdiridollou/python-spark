from pyspark import SparkContext, SparkConf

def is_not_header(line: str):
    return not (line.startswith("hosts") and "bytes" in line)


if __name__ == "__main__":

    '''
    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
    "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
    Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
    take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

    Keep in mind, that the original log files contains the following header lines.
    host    logname    time    method    url    response    bytes

    Make sure the head lines are removed in the resulting RDD.
    '''
    conf = SparkConf().setAppName("union_logs").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    july_first_logs = sc.textFile("in/nasa_19950701.tsv")
    august_first_logs = sc.textFile("in/nasa_19950801.tsv")

    aggregated_logs = july_first_logs.union(august_first_logs)

    clean_log_lines = aggregated_logs.filter(is_not_header)
    sample = clean_log_lines.sample(withReplacement=True, fraction=0.1)
    
    sample.saveAsTextFile("out/sample_nasa_logs.csv")