import pandas as pd
import random
import os


def generate_sample_with_percentage(p=0.0001):
    path_parent = os.path.dirname(os.getcwd())
    os.chdir(path_parent)
    print(": " + os.getcwd() + "\\data\\steam_sample_" + str(p * 100) + "_percents.csv will be saved")
    df = pd.read_csv(
        os.getcwd() + "\\data\\steam_reviews.csv",
        header=0,
        skiprows=lambda i: i > 0 and random.random() > p,
        index_col=[0]
    )
    df.to_csv(os.getcwd() + "\\data\\steam_sample_" + str(p * 100) + "_percents.csv")


if __name__ == '__main__':
    generate_sample_with_percentage(0.0003)
