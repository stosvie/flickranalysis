import datetime
from datetime import date, timedelta
import time
import timeit

from urllib import parse

import flickrapi

import pandas as pd
import numpy as np
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import webbrowser
from dateutil import parser


class db:
    engine = None
    connection = None

    def connect(self, server, dbname, username, pwd):

        connecting_string = 'DRIVER={ODBC Driver 17 for SQL Server};Server=%s;Database=%s;UID=%s;PWD=%s;TDS_Version=8.0;Port=1433;'
        connecting_string = connecting_string % (server, dbname, username, pwd)
        params = parse.quote_plus(connecting_string)

        self.engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        self.connection = self.engine.connect()


class flickrtodb:
    _userid = u''
    _apikey = u''
    _secret = u''
    flickr = None
    sqldb = db()

    def __init__(self, userid, apikey, secret ):
        self._userid = userid
        self._apikey = api_key
        self._secret = secret
        self._flickr = None
        pass

    def init(self):
        self.flickr_authenticate()
        self.sqldb.connect('tcp:woo.database.windows.net,1433', 'BYWS', 'boss', 's7#3QzOsB$J*^v3')



    def flickr_authenticate(self):
        self._flickr = flickrapi.FlickrAPI(self._apikey, self._secret, format='parsed-json')
        print('Step 1: authenticate')

        # Only do this if we don't have a valid token already
        if not self._flickr.token_valid(perms='read'):
            # Get a request token
            self._flickr.get_request_token(oauth_callback='oob')

            # Open a browser at the authentication URL. Do this however
            # you want, as long as the user visits that URL.
            authorize_url = self._flickr.auth_url(perms='read')
            print(authorize_url)
            webbrowser.open_new_tab(authorize_url)

            # Get the verifier code from the user. Do this however you
            # want, as long as the user gives the application the code.
            verifier = str(input('Verifier code: '))

            # Trade the request token for an access token
            self._flickr.get_access_token(verifier)

    def get_stats_batch(self):

        ls = self.get_dates()
        for dt in ls:
            ps = self.get_photo_stats(dt[0])
        print(ls)

    def get_dates(self):

        results = ()
        try:
            cursor = self.sqldb.connection.execute("EXEC fs.GetDatesToLoad")
            # fetch result parameters
            results = list(cursor.fetchall())
            cursor.close()
        except:
            raise
        finally:
            return results

    def _get_photo_domains(self, df, dom_func, referrers_func,dt):

        df_domains = pd.DataFrame()
        for ph in df:
            popular = dom_func(date=dt, per_page=100, page=1, photo_id=ph)

            if int(popular['domains']['pages']) > 0:

                print("""Photo with id {} \
                                reports total of {} pages \
                                and has a list of {} domains,\
                                total is {}""".format(ph,
                                                    popular['domains']['pages'],
                                                    len(popular['domains']['domain']),
                                                    popular['domains']['total']))
                df_domains = df_domains.append(pd.DataFrame(popular['domains']['domain']))
                dt_domain = [dt for i in range(df_domains.index.size)]
                df_domains['statdate'] = dt_domain
                ph_domain = [ph for i in range(df_domains.index.size)]
                df_domains['photoid'] = ph_domain
                for dom in df_domains['name']:
                    refs = referrers_func(date=dt, photo_id = ph, domain=dom, per_page=100, page=1)
                    df_refs = pd.DataFrame(refs['domain']['referrer'])

                while popular['domains']['pages'] - popular['domains']['page'] > 0:
                    popular = dom_func(date=dt, per_page=100, page=popular['domains']['page']+1, photo_id=ph)
                    df_domains = df_domains.append(pd.DataFrame(popular['domains']['domain']))
                    dt_domain = [dt for i in range(df_domains.index.size)]
                    df_domains['statdate'] = dt_domain
                    ph_domain = [ph for i in range(df_domains.index.size)]
                    df_domains['photoid'] = ph_domain


        return df_domains

    def _get_domains(self, dom_func, referrers_func, d):
        res = self._domains_helper(dom_func, referrers_func, d, 1)
        final_df = res[0]

        while res[1] - res[2] > 0:
            res = self._domains_helper(dom_func, referrers_func, d, res[2] + 1)
            final_df = final_df.append(res[0])

        dt_list = [d for i in range(final_df.index.size)]
        final_df['statdate'] = dt_list
        return final_df

    def _domains_helper(self, dom_func, referrers_func, dt, pg):
        popular = dom_func(date=dt, per_page=100, page=pg)
        final_outer = pd.DataFrame()
        if int(popular['domains']['pages']) > 0:
            df_domains = pd.DataFrame(popular['domains']['domain'])

            for dom in popular['domains']['domain']:
                refs = referrers_func(date=dt, domain=dom['name'], per_page=100, page=1)
                df_refs = pd.DataFrame(refs['domain']['referrer'])
                dt_domain = [dom['name'] for i in range(df_refs.index.size)]
                df_refs['domain'] = dt_domain
                final_df = df_refs
                while refs['domain']['pages'] - refs['domain']['page'] > 0:
                    refs = referrers_func(date=dt, domain=dom['name'], per_page=100, page=refs['domain']['page'] + 1)
                    df_refs = pd.DataFrame(refs['domain']['referrer'])
                    dt_domain = [dom['name'] for i in range(df_refs.index.size)]
                    df_refs['domain'] = dt_domain
                    final_df = final_df.append(df_refs)

                final_outer = final_outer.append(final_df)

        if not 'searchterm' in final_outer.columns:
            searchterm = [None for i in range(final_outer.index.size)]
            final_outer['searchterm'] = searchterm

        return final_outer, popular['domains']['pages'], popular['domains']['page']

    def get_photo_stats(self, dt):
        popular = self._flickr.stats.getPopularPhotos(date=dt, per_page=100, page=0)
        df_popular = pd.DataFrame(popular['photos']['photo'])

        while popular['photos']['pages'] - popular['photos']['page'] > 0:
            popular = self._flickr.stats.getPopularPhotos(date=dt, per_page=100, page=popular['photos']['page']+1)
            df_popular = df_popular.append(pd.DataFrame(popular['photos']['photo']))

        df_popular = df_popular[['id', 'title', 'stats']]
        df_stats = pd.DataFrame(df_popular['stats'].values.tolist())
        df_popular.reset_index(inplace=True,drop=True)
        df_popular = df_popular.join(df_stats)
        df_popular.drop('stats', axis=1, inplace=True)

        dt_list = [dt for i in range(df_popular.index.size)]
        df_popular['statdate'] = dt_list

        photo_domain_stats = self._get_domains(self._flickr.stats.getPhotoDomains,
                                               self._flickr.stats.getPhotoReferrers, dt)
        t = df_popular['id']
        retval = self._get_photo_domains(t, self._flickr.stats.getPhotoDomains,
                                                 self._flickr.stats.getPhotoReferrers, dt)

        print(df_popular)
        return (df_popular, photo_domain_stats )



api_key = u'4a69280a31fc96c26e7d218c3a8cf345'
api_secret = u'a2d9a639fd955497'
myuserid = u'58051209@N00'

x = flickrtodb(myuserid, api_key, api_secret)
x.init()
x.get_stats_batch()
