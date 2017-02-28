# -*- coding: utf-8 -*-

import os
import pandas as pd

from ddf_utils.str import to_concept_id
from ddf_utils.index import get_datapackage


# config of path
codebook = '../source/Indicator_labels_for FLUDE.xlsx'
source_dir = '../source/'
out_dir = '../../'


def read_data():
    """read all csv data into a list"""
    all_data = []

    for f in os.listdir(source_dir):
        if 'csv' in f:
            if f == '9. ANNUAL DATA.csv':
                df = pd.read_csv(os.path.join(source_dir, f),
                                 encoding='iso-8859-1',
                                 sep=';', decimal=',',
                                 skipinitialspace=True,
                                 na_values=['n.a.', 'n/a'])
            else:
                df = pd.read_csv(os.path.join(source_dir, f),
                                 encoding='iso-8859-1',
                                 skipinitialspace=True,
                                 na_values=['n.a.', 'n/a'],
                                 thousands=',')
            df = df.loc[:, 'Country':]

            all_data.append(df)

    return all_data


if __name__ == '__main__':
    cb = pd.read_excel(codebook)
    all_data = read_data()

    # entities
    # assuming all csv have same country entities
    assert len(set([len(df['Country'].drop_duplicates()) for df in all_data])) == 1, \
        "Country eneities are different in different source files!"

    country = all_data[0][['Country', 'Name']]
    country = country.drop_duplicates().copy()
    country.columns = ['country', 'name']
    country['country'] = country['country'].map(to_concept_id)
    country.to_csv(os.path.join(out_dir, 'ddf--entities--country.csv'), index=False)

    # concepts
    conc = cb[['Indicator', 'Label', 'Unit of measure']].copy()
    conc.columns = ['concept', 'name', 'unit']
    conc['concept'] = conc['concept'].map(to_concept_id)
    conc['concept_type'] = 'measure'
    conc.to_csv(os.path.join(out_dir, 'ddf--concepts--continuous.csv'), index=False)

    disc = pd.DataFrame([['name', 'Name', 'string'], ['year', 'Year', 'time'],
                         ['country', 'Country', 'entity_domain'], ['unit', 'Unit', 'string']
                         ], columns=['concept', 'name', 'concept_type'])
    disc.to_csv(os.path.join(out_dir, 'ddf--concepts--discrete.csv'), index=False)

    # datapoints
    dps = {}

    for df in all_data:
        df.columns = list(map(to_concept_id, df.columns))
        df['country'] = df['country'].map(to_concept_id)

        df = df.set_index(['country', 'year'])

        for c in df.columns:
            if c in conc['concept'].values:
                if c not in dps.keys():
                    df_ = df[c].dropna().reset_index().copy()
                    dps[c] = df_

                else:
                    df_ = df[c].dropna().reset_index().copy()
                    dps[c] = pd.concat([dps[c], df_]).drop_duplicates()

    for k, df in dps.items():
        path = os.path.join(out_dir, 'ddf--datapoints--{}--by--country--year.csv'.format(k))
        df.to_csv(path, index=False)

    # datapackage
    get_datapackage(out_dir, use_existing=True, to_disk=True)

    print('Done.')
