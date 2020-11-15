import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from commons.Utils import Utils

def splitComma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[2])


if __name__ == "__main__":

    '''
    Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
    and output the airport's name and the city's name to out/airports_in_usa.text.

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:
    "Putnam County Airport", "Greencastle"
    "Dowagiac Municipal Airport", "Dowagiac"
    ...
    '''
    conf = SparkConf().setAppName("airport").setMaster("local[2]")
    sc = SparkContext(conf = conf)
    
    airports = sc.textFile("in/airports.text")
    airports_in_usa = airports.filter(lambda line: Utils.COMMA_DELIMITER.split(line)[3] == "\"United States\"")

    airports_name_and_city = airports_in_usa.map(splitComma)
    airports_name_and_city.saveAsTextFile("out/airports_in_usa.text")
