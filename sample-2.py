#
# @Author: Bhaskar S
# @Blog:   https://www.polarsparc.com
# @Date:   07 Oct 2022
#

import logging
import pyarrow.parquet as pq

logging.basicConfig(format='%(levelname)s %(asctime)s - %(message)s', level=logging.INFO)


def read_uncompressed_parquet(path):
    logging.info('Ready to read the Palmer Penguins dataset from Uncompressed Parquet...\n')
    pq_unc = pq.ParquetFile(path)
    logging.info('Schema -> %s', pq_unc.schema)
    logging.info('No. of Row Groups -> %d\n', pq_unc.num_row_groups)
    logging.info('Metadata -> %s\n', pq_unc.metadata)
    logging.info('First Row Group (Metadata) -> %s\n', pq_unc.metadata.row_group(0))
    logging.info('First Row Group (Content) -> %s\n', pq_unc.read_row_group(0))
    logging.info('First Row Group, Second Column Chunk (Metadata) -> %s', pq_unc.metadata.row_group(0).column(1))
    logging.info('Completed reading the Palmer Penguins dataset from Uncompressed Parquet...')


def read_compressed_parquet(path):
    logging.info('Ready to read the Palmer Penguins dataset from Compressed Parquet...\n')
    pq_com = pq.ParquetFile(path)
    logging.info('Schema -> %s', pq_com.schema)
    logging.info('No. of Row Groups -> %d\n', pq_com.num_row_groups)
    logging.info('Metadata -> %s\n', pq_com.metadata)
    logging.info('First Row Group (Metadata) -> %s\n', pq_com.metadata.row_group(0))
    logging.info('First Row Group (Content) -> %s\n', pq_com.read_row_group(0))
    logging.info('First Row Group, Second Column Chunk (Metadata) -> %s', pq_com.metadata.row_group(0).column(1))
    logging.info('Completed reading the Palmer Penguins dataset from Compressed Parquet...')


def main():
    read_uncompressed_parquet('./data/unc_pp.parquet')
    logging.info('\n')
    logging.info('--------------------------------------------------\n')
    read_compressed_parquet('./data/com_pp.parquet')


if __name__ == '__main__':
    main()
