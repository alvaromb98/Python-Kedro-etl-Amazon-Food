from kedro.io import DataCatalog, MemoryDataSet
from pyspark.sql import DataFrame

data_catalog = DataCatalog({"number_of_ratings": MemoryDataSet()})


def number_of_ratings_per_product(amazon_food: DataFrame) -> DataFrame:
    number_of_ratings = amazon_food.groupBy('ProductId') \
        .avg('Score') \
        .count() \
        .filter('count' > 10) \
        .show()
    return number_of_ratings


def best_food_list(number_of_ratings: DataFrame) -> DataFrame:
    best_food_sorted = number_of_ratings.sort('avg', 'count').limit(10)
    return best_food_sorted


def worst_food_list(number_of_ratings: DataFrame) -> DataFrame:
    worst_food_sorted = number_of_ratings.sort('avg', 'count').tail(10)
    return worst_food_sorted


def count_clients(amazon_food: DataFrame) -> DataFrame:
    top_clients = amazon_food.groupBy('UserId').count.select('UserId').show(10)
    return top_clients
