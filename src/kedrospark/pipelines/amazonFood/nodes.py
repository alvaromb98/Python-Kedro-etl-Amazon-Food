from kedro.io import DataCatalog, MemoryDataSet
from pyspark.sql import DataFrame

data_catalog = DataCatalog({"food_ranked": MemoryDataSet()})


def count_clients(amazon_food: DataFrame) -> DataFrame:
    top_clients = amazon_food.groupBy('UserId').count().select('UserId').limit(10)
    return top_clients


def best_food_list(amazon_food: DataFrame) -> DataFrame:
    amazon_food_sorted = amazon_food.sort("Score")
    no_duplicates = amazon_food_sorted.dropDuplicates("ProductId")
    top_ten_best_food = no_duplicates.select('productName', 'productId', 'Text').limit(10)
    return top_ten_best_food


def worst_food_list(amazon_food: DataFrame) -> DataFrame:
    amazon_food_sorted = amazon_food.sort("Score")
    no_duplicates = amazon_food_sorted.dropDuplicates("ProductId")
    top_ten_worst_food = no_duplicates.select('productName', 'productId', 'Text').orderBy('count').limit(10)
    return top_ten_worst_food
