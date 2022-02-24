from kedro.io import DataCatalog, MemoryDataSet
from pandas import DataFrame
import pandas as pd

data_catalog = DataCatalog({"number_of_ratings": MemoryDataSet()})


def number_of_ratings_per_product(amazon_food: DataFrame) -> DataFrame:
    number_of_ratings = amazon_food.groupby('productName') \
        .agg({'ProductId': 'size', 'Score': 'mean'}) \
        .rename(columns={'ProductId': 'count', 'Score': 'Average rating'}) \
        .reset_index() \
        .query('count>30')
    return number_of_ratings


def best_food_list(number_of_ratings: DataFrame) -> DataFrame:
    best_food_sorted = number_of_ratings.sort_values(['Average rating', 'count'], ascending=False)
    return best_food_sorted


def worst_food_list(number_of_ratings: DataFrame) -> DataFrame:
    worst_food_sorted = number_of_ratings.sort_values(['Average rating', 'count'], ascending=True)
    return worst_food_sorted


def count_clients(amazon_food: DataFrame) -> DataFrame:
    top_clients = amazon_food.groupby('UserId')\
        .agg({'UserId': 'size'})\
        .rename(columns={'UserId': 'count'})\
        .reset_index()\
        .sort_values('count', ascending=False)
    return top_clients
