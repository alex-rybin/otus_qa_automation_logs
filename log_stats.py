import argparse
import json
import os

import pandas as pd
from envparse import env

env.read_envfile()
parser = argparse.ArgumentParser()
parser.add_argument(
    '-p', '--path', type=str, required=True, help='Path to log file or directory'
)
parser.add_argument(
    '-r',
    '--result',
    type=str,
    default=env.str('RESULT_PATH'),
    help='Path to result JSON file',
)
args = parser.parse_args()
log_path = args.path

if os.path.isfile(log_path):
    log_data = pd.read_csv(log_path, header=None, sep='\s+')
elif os.path.isdir(log_path):
    file_paths = [
        file
        for file in os.listdir(log_path)
        if os.path.isfile(os.path.join(log_path, file))
    ]
    if not file_paths:
        raise ValueError(f'Directory does cot contain files: {log_path}')
    dataframes = [
        pd.read_csv(os.path.join(log_path, file_path), header=None, sep='\s+')
        for file_path in file_paths
    ]
    log_data = pd.concat(dataframes, ignore_index=True)
else:
    raise ValueError('Provided path is not a valid file or directory')

log_data = log_data[[0, 3, 5, 6, 7]]
log_data[['Method', 'Link Path']] = log_data[5].str.split(n=2, expand=True)[[0, 1]]
log_data[3] = pd.to_datetime(log_data[3], format='[%d/%b/%Y:%H:%M:%S')
log_data = log_data[[0, 3, 'Method', 'Link Path', 6, 7]]
log_data = log_data.rename(
    columns={0: 'IP', 3: 'Date', 6: 'Status Code', 7: 'Response Size'}
)
log_data['Status Code'] = pd.to_numeric(log_data['Status Code'])
log_data['Response Size'] = (
    pd.to_numeric(log_data['Response Size'], 'coerce').fillna(0).astype(int)
)

stats = {
    'total_requests': log_data.shape[0],
    'requests_by_method': log_data['Method'].value_counts().to_dict(),
    'top_10_ip_by_request_count': log_data['IP'].value_counts()[:10].to_dict(),
    'top_10_requests_by_size': log_data.sort_values(
        by=['Response Size'], ascending=False
    )[:10][['Method', 'Link Path', 'Status Code', 'IP']].values.tolist(),
    'last_10_requests_with_client_error': log_data[
        log_data['Status Code'].between(400, 499)
    ]
    .sort_values(by=['Date'], ascending=False)[:10][
        ['Method', 'Link Path', 'Status Code', 'IP']
    ]
    .values.tolist(),
    'last_10_requests_with_server_error': log_data[
        log_data['Status Code'].between(500, 599)
    ]
    .sort_values(by=['Date'], ascending=False)[:10][
        ['Method', 'Link Path', 'Status Code', 'IP']
    ]
    .values.tolist(),
}
with open(args.result, 'wt') as stats_file:
    json.dump(stats, stats_file, indent=4)
