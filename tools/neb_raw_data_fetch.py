import sys
import joblib
import kauffman
import pandas as pd
import database_update.constants as c

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option('chained_assignment',None)

def table_update():
    joblib.dump(str(pd.to_datetime('today')), c.filenamer('../neb/data/raw_data/raw_data_fetch_time.pkl'))

    for region in ['us', 'state']:
        kauffman.bfs(['BA_BA', 'BF_SBF8Q', 'BF_DUR8Q'], region, annualize=True). \
            rename(columns={'BF_DUR8Q': 'avg_speed_annual', 'BF_SBF8Q': 'bf', 'BA_BA': 'ba'}).\
            to_csv(c.filenamer(f'../neb/data/raw_data/bfs_{region}.csv'), index=False)

        kauffman.bfs(['BF_SBF8Q'], region, march_shift=True). \
            rename(columns={'BF_SBF8Q': 'bf_march_shift'}). \
            to_csv(c.filenamer(f'../neb/data/raw_data/bfs_march_{region}.csv'), index=False)

        kauffman.pep(region). \
            rename(columns={'POP': 'population'}). \
            astype({'time': 'int', 'population': 'int'}).\
            to_csv(c.filenamer(f'../neb/data/raw_data/pep_{region}.csv'), index=False)

        kauffman.bds(['FIRM'], obs_level=region). \
            rename(columns={'FIRM': 'firms'}).\
            to_csv(c.filenamer(f'../neb/data/raw_data/bds_{region}.csv'), index=False)


def main():
    table_update()


if __name__ == '__main__':
    main()
