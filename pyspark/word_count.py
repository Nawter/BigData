from pyspark import SparkContext
sc =SparkContext()
text_file = sc.textFile("word_input.txt")
#counts = text_file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
counts = text_file.flatMap(lambda line: line.split(" "))
print counts.collect()
words = counts.map(lambda word: (word, 1))
print words.collect()
#counts.saveAsTextFile("word_input.txt")
