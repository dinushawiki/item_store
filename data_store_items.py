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

sales = spark.read.option("header","true").option("inferSchema","true").csv("sales.csv")
recipe = spark.read.option("header","true").option("inferSchema","true").csv("recipe.csv")

# storeItems.createOrReplaceTempView("storeItems")
# recentItems = spark.sql("select * from storeItems where date > current_timestamp() - interval 3 month")

saleItems = sales.join(recipe,sales["Item"]==recipe["prepared_item"],how="left") \
    .withColumn("total_units",func.when(recipe["units"].isNotNull(),sales["Quantity"]*recipe["units"]).otherwise(sales["Quantity"]))

joined = saleItems.join(storeItemsWithPrice,(saleItems["Date"]==storeItems["Date"]) & \
                        (saleItems["Store"]==storeItems["Store"]) & \
                            (saleItems["Store"]==storeItems["Store"]) & \
                                (saleItems["Item"]==storeItems["Item"]) & (saleItems["Promo Type"]==storeItems["Promo Type"]),how="left")

storeItems.show(100)
saleItems.show(100)
joined.show(100)






