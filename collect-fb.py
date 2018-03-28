import configparser
from ATB.ATB.Facebook import Facebook

import datetime
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
import requests
from requests_futures.sessions import FuturesSession
from tqdm import tqdm
import urllib
import json
import argparse


# Facebook
def fb_query(url):
    og_object = None
    og_engagement = None
    og_error = None

    try:
        fb_response = fb_graph.get_object(
            id=url,
            fields="engagement, og_object"
        )

        if 'og_object' in fb_response:
            og_object = fb_response['og_object']
        if 'engagement' in fb_response:
            og_engagement = fb_response['engagement']
    except Exception as e:
        og_error = e

    return (og_object, og_engagement, og_error)

# Facebook


def fb_queries(urls):
    results = {}

    try:
        responses = fb_graph.get_objects(
            ids=urls,
            fields="engagement, og_object"
        )
    except:
        raise

    for url, r in responses.items():
        og_object = None
        og_engagement = None
        og_error = None

        if 'og_object' in r:
            og_object = r['og_object']
        if 'engagement' in r:
            og_engagement = r['engagement']
        results[url] = (og_object, og_engagement, og_error)

    return results


def init_config(config):
    Config = configparser.ConfigParser()
    Config.read(config)
    FACEBOOK_APP_ID = Config.get('facebook', 'app_id')
    FACEBOOK_APP_SECRET = Config.get('facebook', 'app_secret')

    return Facebook(app_id=FACEBOOK_APP_ID, app_secret=FACEBOOK_APP_SECRET)


def load_datasets():
    dfs = {}

    dfs["cr"] = pd.read_csv("data/crossref_100k_resolved.csv")
    dfs["cr"] = dfs["cr"].set_index('doi')

    # dfs["upw"] = pd.read_csv("data/unpaywall_100k_resolved.csv")
    # dfs["upw"] = dfs["upw"].set_index('doi')

    return dfs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Query FB URL with DOI-URLs')
    parser.add_argument('-i', '--input', required=True,
                        help='Specifiy input file')
    parser.add_argument('-U', '--url', dest='url', action='store_true',
                        help='Constructs HTTP/HTTPS pairs \
                             (doubles collect time)')
    parser.add_argument('-p', '--parallel', default=50, type=int,
                        help='Number of parallel requests (default/max=50)')
    args = vars(parser.parse_args())

    # Init Facebook
    fb_graph = init_config("config.cnf")

    # Init dataset
    df = pd.read_csv(args['input'])
    df = df.set_index('doi')
    df = df[df.resolved.notnull() & (~df.resolved.duplicated())]

    # Create alternative URLs
    print("All URLs are http or https: {}".format(
        len(df) == df.resolved.map(lambda x: x[:4] == "http").sum()
    ))
    df['url'] = df.resolved
    if args['url']:
        df['url2'] = df.url.map(lambda x: x[:4] + x[5:]
                                if x[4] == "s" else x[:4] + "s" + x[4:])
    print("#DOIs: ", len(df))

    # Create temp out
    if args['url']:
        out_df = df[['url', 'url2']].copy()
        for col in ['og_id', 'og_obj', 'og_eng', 'og_err',
                    'og_id_alt', 'og_obj_alt', 'og_eng_alt', 'og_err_alt',
                    'ts']:
            out_df[col] = None
    else:
        out_df = df[['url']].copy()
        for col in ['og_id', 'og_obj', 'og_eng', 'og_err', 'ts']:
            out_df[col] = None

    batchsize = args['parallel']
    indices = list(out_df.index)

    failed_ind = []
    for i in tqdm(range(0, len(indices), batchsize), desc="Collecting in batches"):
        bad_ind = indices[i:i+batchsize]
        batch = out_df.loc[bad_ind]

        now = datetime.datetime.now()

        # Query and process fb_queries for original URL
        try:
            results = fb_queries(
                [x for x in batch.url.tolist() if pd.notnull(x)])
        except:
            failed_ind.extend(bad_ind)

        for url, resp in results.items():
            if resp[0]:
                out_df.loc[out_df.url == url,
                           'og_id'] = json.dumps(resp[0]['id'])
                out_df.loc[out_df.url == url, 'og_obj'] = json.dumps(resp[0])
            if resp[1]:
                out_df.loc[out_df.url == url, 'og_eng'] = json.dumps(resp[1])
            if resp[2]:
                out_df.loc[out_df.url == url, 'og_err'] = str(resp[2])
            out_df.loc[out_df.url == url, 'ts'] = str(now)

        # Query and process fb_queries for alternative URL
        if args['url']:
            try:
                results_alt = fb_queries(
                    [x for x in batch.url2.tolist() if pd.notnull(x)])
            except:
                failed_ind.extend(bad_ind)

            for url, resp in results_alt.items():
                if resp[0]:
                    out_df.loc[out_df.url2 == url,
                               'og_id_alt'] = json.dumps(resp[0]['id'])
                    out_df.loc[out_df.url == url,
                               'og_obj_alt'] = json.dumps(resp[0])
                if resp[1]:
                    out_df.loc[out_df.url2 == url,
                               'og_eng_alt'] = json.dumps(resp[1])
                if resp[2]:
                    out_df.loc[out_df.url2 == url, 'og_err_alt'] = str(resp[2])

    # Individually re-query failed rows
    rows = list(out_df.loc[list(set(failed_ind))].itertuples())
    for row in tqdm(rows, desc="Collecting failed rows individually"):
        url = row.url
        resp = fb_query(url)
        if resp[0]:
            out_df.loc[out_df.url == url,
                       'og_id'] = json.dumps(resp[0]['id'])
            out_df.loc[out_df.url == url, 'og_obj'] = json.dumps(resp[0])
        if resp[1]:
            out_df.loc[out_df.url == url, 'og_eng'] = json.dumps(resp[1])
        if resp[2]:
            out_df.loc[out_df.url == url, 'og_err'] = str(resp[2])
        out_df.loc[out_df.url == url, 'ts'] = str(now)

        if args['url']:
            url = row.url2
            resp = fb_query(url)
            if resp[0]:
                out_df.loc[out_df.url == url,
                           'og_id'] = json.dumps(resp[0]['id'])
                out_df.loc[out_df.url == url, 'og_obj'] = json.dumps(resp[0])
            if resp[1]:
                out_df.loc[out_df.url == url, 'og_eng'] = json.dumps(resp[1])
            if resp[2]:
                out_df.loc[out_df.url == url, 'og_err'] = str(resp[2])
            out_df.loc[out_df.url == url, 'ts'] = str(now)

    if args['url']:
        out_df.to_csv(args['input'].split(".csv")[0] + "_facebook_double.csv")
    else:
        out_df.to_csv(args['input'].split(".csv")[0] + "_facebook.csv")
