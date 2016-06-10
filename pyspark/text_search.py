from pyspark import SparkContext
sc =SparkContext()
text_file = sc.textFile("dse10.l4.log")
errors = text_file.filter(lambda line: "ERROR" in line)
errors.cache()
# Count all the errors
print "Number of words with error %d" % errors.count()
# visualize all the errors
print "List of words with error %s" % errors.collect()
# Count errors mentioning MySQL
errors = errors.filter(lambda line: "ARM1100016" in line)
print "Number of words with ARM1100016 %d" % errors.count()
# Fetch the MySQL errors as an array of strings
print "List of words with error %s" % errors.filter(lambda line: "ARM1100016" in line).collect()
