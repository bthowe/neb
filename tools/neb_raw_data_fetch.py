import sys
import boto3
import joblib
import pandas as pd
import constants as c
from kauffman.tools import file_to_s3
from kauffman.data import bfs, pep, bds

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option('chained_assignment',None)


def raw_data_update():
    joblib.dump(str(pd.to_datetime('today')), c.filenamer('data/raw_data/raw_data_fetch_time.pkl'))

    for region in ['us']:
    # for region in ['us', 'state']:
        bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], region, industry='31-33', annualize=True). \
            rename(columns={'BF_DUR8Q': 'avg_speed_annual', 'BF_SBF8Q': 'bf', 'BA_BA': 'ba'}).\
            to_csv(c.filenamer(f'data/raw_data/bfs_{region}.csv'), index=False)

        bfs(['BF_SBF8Q'], region, industry='31-33', march_shift=True). \
            rename(columns={'BF_SBF8Q': 'bf_march_shift'}). \
            to_csv(c.filenamer(f'data/raw_data/bfs_march_{region}.csv'), index=False)

        pep(region). \
            rename(columns={'POP': 'population'}). \
            astype({'time': 'int', 'population': 'int'}).\
            to_csv(c.filenamer(f'data/raw_data/pep_{region}.csv'), index=False)

        bds(['FIRM'], strata=['NAICS'], obs_level=region, census_key='1ab6c1ad36246279fa7d5fcaff693f65c5537c08'). \
            rename(columns={'FIRM': 'firms'}).\
            query('naics == "31-33"').\
            to_csv(c.filenamer(f'data/raw_data/bds_{region}.csv'), index=False)


def s3_update():
    files_lst = [
        'raw_data_fetch_time.pkl', 'bfs_us.csv', 'bfs_march_us.csv', 'pep_us.csv', 'bds_us.csv', 'bfs_state.csv',
        'bfs_march_state.csv', 'pep_state.csv', 'bds_state.csv'
    ]

    for file in files_lst:
        file_to_s3(c.filenamer(f'data/raw_data/{file}'), 'emkf.data.research', f'indicators/neb/raw_data/{file}')


def main():
    raw_data_update()
    # s3_update()


if __name__ == '__main__':
    main()
