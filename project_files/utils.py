import yaml

def read_yaml(file_path):
    """
    Read Config file in YAML format
    :param file_path: file path to config.yaml
    :return: dictionary with config details
    """
    with open(file_path, "r") as f:
        return yaml.safe_load(f)

def load_csv_to_df(spark, file_path) :
    """
    Read CSV data
    spark: spark instance
    file_path: path to the csv file
    :return: dataframe
    """
    return spark.read.option("inferSchema", "true").csv(file_path, header=True)


def write_out(df, f_path, write_format):

    """
    Write data frame to csv
    :param write_format: Write file format
    :param df: dataframe
    :param f_path: output file path
    :return: None
    """
    # df = df.coalesce(1)
    df.repartition(1).write.format(write_format).mode("overwrite").option(
        "header", "true"
    ).save(f_path)