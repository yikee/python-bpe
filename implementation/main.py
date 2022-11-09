from pyspark.sql import SparkSession
import json, process, bpe

def main(input, output, task='', **kwargs):
    output = join_path(output, task)

    raw_word_dict_path = join_path(output, 'raw_word_dict')

    spark = SparkSession \
            .builder \
            .enableHiveSupport() \
            .getOrCreate()
    #sc = spark.sparkContext

    #df = spark.read.csv('/user/819/report_desc_14days.csv', encoding='gb18030')
    #df = df.collect()

    word_dict = {}
    max_epoch = 100
    max_merge_time = 5000
    merge_freq_require = 30
    num_slices = 100

    # rdd = sc.parallelize(data, numSlices=num_slices)
    rdd = spark.sql(get_sql()).rdd.flatMap(process).coalesce(num_slices)

    for epoch in range(1, max_epoch+1):
        last_epoch = False
        print("epoch:", epoch)
        if epoch == max_epoch:
            last_epoch = True
        rdd = rdd.mapPartitions(lambda sub_data: bpe(sub_data, last_epoch, max_merge_time, merge_freq_require))
        if last_epoch:
            #word_dict = rdd.reduce(merge_dict)
            rdd.toDF(['word2freq']).write.mode('overwrite').text(raw_word_dict_path)
        else:
            rdd = rdd.repartition(num_slices)

    rdd = spark.read.text(raw_word_dict_path).rdd.map(lambda x: json.loads(x.values()))
    word_dict = rdd.reduce(merge_k_v)
    word_dict = preprocess(word_dict)

    word2freq = sorted(word_dict.items(), key=lambda x: -x[1])
    word2freq = [Row(text='%s %s' % (w, f)) for w, f in word2freq]
    spark.createDataFrame(word2freq).write.mode('overwrite').text(output)
    spark.stop()