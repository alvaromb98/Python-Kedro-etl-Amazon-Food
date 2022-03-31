from kedro.pipeline import Pipeline, node
from .nodes import number_of_ratings_per_product, best_food_list, worst_food_list, count_clients


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=number_of_ratings_per_product,
                inputs="amazon_food_spark",
                outputs="number_of_ratings_spark",
                name="number_of_ratings_per_product_node_spark",
            ),
            node(
                func=worst_food_list,
                inputs="number_of_ratings_spark",
                outputs="top_ten_worst_food_spark",
                name="worst_food_list_node_spark",
            ),
            node(
                func=best_food_list,
                inputs="number_of_ratings_spark",
                outputs="top_ten_best_food_spark",
                name="best_food_list_node_spark",
            ),
            node(
                func=count_clients,
                inputs="amazon_food_spark",
                outputs="top_clients_spark",
                name="count_clients_node_spark",
            )
        ]
    )
