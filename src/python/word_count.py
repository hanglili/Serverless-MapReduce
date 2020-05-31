from serverless_mr.main import ServerlessMR

# from word_count_map import produce_counts
# from word_count_reduce import aggregate_counts
from word_count.map import produce_counts
from word_count.reduce import aggregate_counts


serverless_mr = ServerlessMR()
serverless_mr.map(produce_counts).reduce(aggregate_counts, 4).run()
