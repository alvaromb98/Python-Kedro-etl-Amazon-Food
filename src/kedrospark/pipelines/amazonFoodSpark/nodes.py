from kedro.io import DataCatalog, MemoryDataSet
from pyspark.sql import *
import pandas as pd
from kedro.extras.datasets.spark import SparkDataSet


data_catalog = DataCatalog({"number_of_ratings": MemoryDataSet()})


def number_of_ratings_per_product(amazon_food_spark: DataFrame) -> DataFrame:
    number_of_ratings = amazon_food_spark.groupBy('productName') \
        .agg({'ProductId': 'count', 'Score': 'avg'}) \
        .where(col('count(ProductId)') > 30)
    return number_of_ratings


def best_food_list(number_of_ratings: DataFrame) -> DataFrame:
    best_food_sorted = number_of_ratings.orderBy(['avg(Score)', 'count(ProductId)'], ascending=False)
    return best_food_sorted


def worst_food_list(number_of_ratings: DataFrame) -> DataFrame:
    worst_food_sorted = number_of_ratings.orderBy(['avg(Score)', 'count(ProductId)'], ascending=True)
    return worst_food_sorted


def count_clients(amazon_food_spark: DataFrame) -> DataFrame:
    top_clients = amazon_food_spark.groupBy('UserId') \
        .agg({'UserId': 'count'}) \
        .orderBy('count(UserId)', ascending=False)
    return top_clients
