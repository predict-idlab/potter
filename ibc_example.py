import pandas as pd
import numpy as np
from sklearn.model_selection import ParameterGrid, train_test_split

from src.utils import calculate_passthroughs, calculate_duration, train_model
from semantic import dataloader,featuretransformer, modelbuilder, Pipeline


@dataloader(output = ['atacq_df'])
def fetch_atacq_data():
    atacq_df = pd.read_parquet('./data/raw/atacq_df.parquet')
    return atacq_df


@dataloader(output = ['eventlog_df'])
def fetch_eventlog_data():
    eventlog_df = pd.read_parquet('./data/raw/eventlog_df.parquet')
    return eventlog_df

@dataloader(output = ['production_df'])
def fetch_production_data():
    production_df = pd.read_parquet('./data/raw/production_df.parquet')
    return production_df


@dataloader(output = ['sto_df'])
def fetch_sto_data():
    sto_df = pd.read_parquet('./data/raw/sto_df.parquet')
    return sto_df

@dataloader(output = ['tracking_df'])
def fetch_tracking_data():
    tracking_df = pd.read_parquet('./data/raw/tracking_df.parquet')
    return tracking_df

@dataloader(output = ['ibc_reg_points'], pertain=['ibc'])
def fetch_ibc_regpoints():
    reg_points = ('20905', '20907')
    return reg_points

@featuretransformer(input=['tracking_df', 'atacq_df', 'ibc_reg_points'], output=['body_duration_df'], pertain=['ibc'])
def ibc_calculate_passthroughs(tracking_df, atacq_df, reg_points):
    tracking_points_df = pd.DataFrame(data={'begin':[reg_points[0]], 'end':reg_points[1]})
    passthrough_dict = calculate_passthroughs(tracking_df, atacq_df, tracking_points_df)
    body_duration_dict = calculate_duration(passthrough_dict)
    body_duration_df = body_duration_dict[reg_points]
    return body_duration_df


@featuretransformer(input=['body_duration_df', 'tracking_df'], output=['feat_label_df'])
def ibc_extract_features(body_duration_df, tracking_df):
    # Remove all rows where end timestamps are earlier than begin timestamps
    body_duration_df = body_duration_df.loc[body_duration_df['end_timestamp'] > body_duration_df['begin_timestamp']]
    # Add tracking features using body duration df
    renamed_body_duration_df = body_duration_df.reset_index().rename(columns={'level_0':'bodynumber', 'level_1':'passthrough'})
    tracking_feat_df = pd.merge(tracking_df.reset_index(), renamed_body_duration_df, how='inner', left_on=['bodyNumber', 'timestamp'], right_on=['bodynumber', 'begin_timestamp'])[['timestamp', 'primerColorCode', 'bodyType', 'flowCode', 'diff_min']]
    # Remove duplicates in tracking_feat_df and set index to timestamp
    features_df = tracking_feat_df.drop_duplicates(keep='first').set_index('timestamp').sort_index()

    # Link body number back to timestamp
    label_series = body_duration_df.loc[body_duration_df['begin_timestamp'].isin(features_df.index)][['begin_timestamp', 'fault']]
    time_label_series = label_series.set_index('begin_timestamp')
    feat_label_df = pd.concat([features_df, time_label_series], axis=1)
    label_series = label_series['fault']
    return feat_label_df

@modelbuilder(input=['feat_label_df'])
def ibc_model(features_df):
    # Get an out-of-sample test set
    train_feat_df, test_feat_df = train_test_split(features_df, test_size=0.20, shuffle=False)
    # Define the gap between train and test set
    embargo_size = 10
    train_feat_df = train_feat_df.iloc[:-embargo_size]

    sel_feats = list(train_feat_df.drop(columns=['fault']).columns)
    print(sel_feats)
    sel_train_feat_df = train_feat_df[sel_feats + ['fault']]
    sel_test_feat_df = test_feat_df[sel_feats + ['fault']]

    # Select hyperparameters with a cross-validation grid search
    best_score = 0
    best_params = None
    best_model_list = None
    parameter_grid = ParameterGrid({'iterations': [1000], 'depth': [8], 'od_type': ['Iter'], 'od_wait': [50], 'eval_metric': ['BalancedAccuracy']})
    for cur_params in parameter_grid:
        print(cur_params)
        cur_score, cur_model_list = train_model(sel_train_feat_df, cur_params, verbose=True)
        # Print new line
        print()
        if cur_score > best_score:
            best_score = cur_score
            best_params = cur_params
            best_model_list = cur_model_list
    print(best_score)
    return best_score, best_params, best_model_list


p = Pipeline()
p.search(pertain="ibc")
model = p.execute()