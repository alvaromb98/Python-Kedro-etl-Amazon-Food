from kedro.pipeline import Pipeline, node
from .nodes import best_food_list, worst_food_list, count_clients


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=best_food_list,
                inputs="amazon_food",
                outputs="top_ten_best_food",
                name="best_food_list_node",
            ),
            node(
                func=worst_food_list,
                inputs="amazon_food",
                outputs="top_ten_worst_food",
                name="worst_food_list_node",
            ),
            node(
                func=count_clients,
                inputs="amazon_food",
                outputs="top_clients",
                name="count_clients_node",
            )
        ]
    )


runner = SequentialRunner()


print(runner.run(pipeline, data_catalog))
