# coding: utf-8
import datetime
import requests
import pandas as pd

from tqdm import tqdm

from concurrent.futures import ProcessPoolExecutor
from requests_futures.sessions import FuturesSession

# Read input file
df = pd.read_csv("wos_100k.csv")
dois = df.doi.tolist()

# Options
batchsize = 100
max_workers = 100
timeout = 5

# Init output df
resolved_dois = pd.DataFrame({'doi': dois,
                              'url': None,
                              'ts': None,
                              'err': None,
                              'err_msg': None,
                              'status_code': None})
resolved_dois = resolved_dois.set_index("doi")

# Split dois into batches
batches = range(0, len(dois), batchsize)

# FutureSession
session = FuturesSession(max_workers=max_workers)
for i in tqdm(batches, total=len(batches)):
    futures = []
    batch = dois[i:i + batchsize]

    # create futures in parallel
    for doi in batch:
        now = datetime.datetime.now()
        future = session.get('https://doi.org/{}'.format(doi),
                             allow_redirects=True,
                             timeout=timeout)

        futures.append({
            "doi": doi,
            "ts": str(now),
            "future": future
        })

    # collect future respones and populate df
    for response in futures:
        resolved_dois.loc[response['doi'], 'ts'] = response['ts']
        err = None
        err_msg = None
        status = None

        try:
            resolved_dois.loc[response['doi'],
                              'url'] = response['future'].result().url
            resolved_dois.loc[response['doi'],
                              'status_code'] = response['future'].result().status_code
        except requests.exceptions.Timeout as ex:
            err_msg = str(ex)
            err = "Timeout"
        except requests.exceptions.TooManyRedirects as ex:
            err_msg = str(ex)
            err = "TooManyRedirects"
        except requests.exceptions.RequestException as ex:
            err_msg = str(ex)
            err = "RequestException"

        resolved_dois.loc[response['doi'], 'err'] = err
        resolved_dois.loc[response['doi'], 'err_msg'] = err_msg

resolved_dois.to_csv("wos_resolved.csv")
