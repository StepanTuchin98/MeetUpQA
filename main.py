import logging

from data_generator.generator import run_generator
from data_generator.utils import get_args
from spark.spark_app import run_spark


def main():
    logging.basicConfig(level=logging.INFO)
    args = get_args()
    run_generator(logging, *args)
    run_spark(*args[2:])


if __name__ == "__main__":
    main()


