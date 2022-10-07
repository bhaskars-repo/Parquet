#
# @Author: Bhaskar S
# @Blog:   https://www.polarsparc.com
# @Date:   07 Oct 2022
#

import logging
import pyarrow.parquet as pq

logging.basicConfig(format='%(levelname)s %(asctime)s - %(message)s', level=logging.INFO)


def select_parquet_columns(file):
    return pq.read_table(file, columns=['species', 'body_mass_g'])


def filter_parquet_columns(file):
    return pq.read_table(file, columns=['species', 'body_mass_g'], filters=[('body_mass_g', '>', 4500)])


def main():
    logging.info('Reading columns species and body_mass_g from the Palmer Penguins dataset from Parquet...')
    df = select_parquet_columns('./data/com_pp.parquet').to_pandas()
    logging.info('Displaying top 5 rows from the filtered data set...\n')
    logging.info(df.head(5))
    logging.info('--------------------------------------------------\n')
    logging.info('Filtering column body_mass_g > 4500 from the Palmer Penguins dataset from Parquet...')
    df = filter_parquet_columns('./data/com_pp.parquet').to_pandas()
    logging.info('Displaying top 5 rows from the data set...\n')
    logging.info(df.head(5))


if __name__ == '__main__':
    main()
