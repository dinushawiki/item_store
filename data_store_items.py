from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("StroreItmes").master("local[*]").getOrCreate()

def clean_price(value):
    if value == "N/A":
        return None
    return float(value.replace("$",""))

clean_price_udf = func.udf(clean_price,StringType())

storeItems = spark.read.option("header","true").option("inferSchema","true").csv("date_store_item_pricing_data.csv")
storeItems = storeItems.withColumn("retail_price",clean_price_udf(storeItems["Retail Price"])) \
                        .withColumn("promo_price",clean_price_udf(storeItems["Promo Price"])) \

storeItemsWithPrice = storeItems.withColumn("price", func.when(storeItems["promo_price"].isNotNull(),storeItems["promo_price"]).otherwise(storeItems["retail_price"]))

storeItems.createOrReplaceTempView("storeItems")

recentItems = spark.sql("select * from storeItems where date > current_timestamp() - interval 3 month")



storeItemsWithPrice.show()
storeItems.printSchema()





