#
# @Author: Bhaskar S
# @Blog:   https://www.polarsparc.com
# @Date:   07 Oct 2022
#

import logging
import pandas as pd

logging.basicConfig(format='%(levelname)s %(asctime)s - %(message)s', level=logging.INFO)


# Load the Palmer Penguins data set, clean missing values and return a dataframe to the data set
def get_palmer_penguins():
    logging.info('Ready to load the Palmer Penguins dataset...')
    url = 'https://vincentarelbundock.github.io/Rdatasets/csv/palmerpenguins/penguins.csv'
    df = pd.read_csv(url)
    logging.info('Ready to cleanse the Palmer Penguins dataset...')
    df = df.drop(df.columns[0], axis=1)
    df = df.drop([3, 271], axis=0)
    df.loc[[8, 10, 11], 'sex'] = 'female'
    df.at[9, 'sex'] = 'male'
    df.at[47, 'sex'] = 'female'
    df.loc[[178, 218, 256, 268], 'sex'] = 'female'
    logging.info('Completed cleansing the Palmer Penguins dataset...')
    return df


# Create an uncompressed Parquet file
def write_uncompressed_csv(path, df):
    logging.info('Ready to write the Palmer Penguins dataset as csv...')
    df.to_csv(path, compression=None, index=False)
    logging.info('Completed writing the Palmer Penguins dataset as csv...')


# Create an uncompressed Parquet file
def write_uncompressed_parquet(path, df):
    logging.info('Ready to write the Palmer Penguins dataset as Uncompressed Parquet...')
    df.to_parquet(path, compression=None, index=False)
    logging.info('Completed writing the Palmer Penguins dataset as Uncompressed Parquet...')


# Create a compressed Parquet file
def write_compressed_parquet(path, df):
    logging.info('Ready to write the Palmer Penguins dataset as Compressed Parquet...')
    df.to_parquet(path, compression='snappy', index=False)
    logging.info('Completed writing the Palmer Penguins dataset as Compressed Parquet...')


def main():
    penguins_df = get_palmer_penguins()
    write_uncompressed_csv('./data/unc_pp.csv', penguins_df)
    write_uncompressed_parquet('./data/unc_pp.parquet', penguins_df)
    write_compressed_parquet('./data/com_pp.parquet', penguins_df)


if __name__ == '__main__':
    main()
