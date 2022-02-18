from kedro.pipeline import Pipeline, node


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=number_of_ratings_per_product,
                inputs="amazon_food",
                outputs="number_of_ratings",
                name="best_food_list_node",
            ),
            node(
                func=worst_food_list,
                inputs="number_of_ratings",
                outputs="top_ten_worst_food",
                name="worst_food_list_node",
            ),
            node(
                func=best_food_list,
                inputs="number_of_ratings",
                outputs="top_ten_best_food",
                name="count_clients_node",
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
