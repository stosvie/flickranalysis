import datetime
from datetime import date, timedelta
import time
import timeit

from urllib import parse

import flickrapi
# from pandas.io.json import json_normalize
import pandas as pd
import numpy as np
import sqlalchemy as sa
import webbrowser

def calldb(data, table_name):

    connecting_string = 'Driver={ODBC Driver 17 for SQL Server};Server=tcp:woo.database.windows.net,1433;Database=BYWS;Uid=boss;Pwd=s7#3QzOsB$J*^v3;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    params = parse.quote_plus(connecting_string)

    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)
    connection = engine.connect()
    data.to_sql(table_name, con=engine, if_exists='append', chunksize=1000)
    # connection.close()

def delete_stats_from_date(livedate):
    connecting_string = 'Driver={ODBC Driver 17 for SQL Server};Server=tcp:woo.database.windows.net,1433;Database=BYWS;Uid=boss;Pwd=s7#3QzOsB$J*^v3;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    params = parse.quote_plus(connecting_string)

    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    connection = engine.connect()
    query = "\
                    delete from dbo.photo_stats_domains where statdate >= '{}';\
                    delete from dbo.photo_stats where statdate >= '{}';\
                    delete from dbo.stats_status where statdate >= '{}';".format(livedate.date(), livedate.date(),livedate.date())

    trans = connection.begin()
    try:
        r1 = connection.execute(query)
        print(r1)
        trans.commit()
    except:
        trans.rollback()
        raise
    finally:
        connection.close()

def mark_date_complete(loaddate):
    connecting_string = 'Driver={ODBC Driver 17 for SQL Server};Server=tcp:woo.database.windows.net,1433;Database=BYWS;Uid=boss;Pwd=s7#3QzOsB$J*^v3;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    params = parse.quote_plus(connecting_string)

    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    connection = engine.connect()

    if loaddate == date.today():
        new_state = 'live'
    else:
        new_state = 'frozen'

    query = "insert into dbo.stats_status (statdate,stats_state)\
        VALUES ('{}','{}')".format(loaddate, new_state)

    trans = connection.begin()
    try:
        r1 = connection.execute(query)
        print(r1)
        trans.commit()
    except:
        trans.rollback()
        raise
    finally:
        connection.close()

def refresh_stats():
    dlist = get_saved_stats()
    get_stats(dlist, dlist[0])
    get_domains(dlist, dlist[0])

def get_saved_stats():
    connecting_string = 'Driver={ODBC Driver 17 for SQL Server};Server=tcp:woo.database.windows.net,1433;Database=BYWS;Uid=boss;Pwd=s7#3QzOsB$J*^v3;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    params = parse.quote_plus(connecting_string)

    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    connection = engine.connect()
    # query = "SELECT * FROM dbo.photo_faves WHERE photo_id = '" + str(pid) + "'"
    query = "select max(statdate)  as stats_date,stats_state from dbo.stats_status group by stats_state \
            union select  cast(getdate() as date) as  statdate, 'today' as stats_state"

    #union select  '2020-02-14' as  statdate, 'live' as stats_state\
    df = pd.read_sql_query(query, engine)  # Finally, importing the data into DataFrame df
    livedate = df[df.stats_state.eq('live')]['stats_date'][1]

    #query = "delete from dbo.photo_stats where statdate >= {}".format(livedate.date())
    #delete_stats_from_date(livedate.date())

    foo = df[df.stats_state.eq('frozen')]['stats_date']
    t = date.today()
    bar = pd.date_range(start=livedate.date(), end=date.today()).tolist()

    return bar
    # ndf = df[df['favedate'] > str(1498608604)]




def test_photos(fobj, myuserid):

    photos = flickr2.people.getPhotos(user_id=myuserid, per_page=100, page=1)

    df2 = pd.DataFrame(photos['photos']['photo'])
    df_dates = pd.DataFrame()
    df_comments = pd.DataFrame()
    for f in photos['photos']['photo']:
        theid = [f['id']]
        i1 = flickr2.photos.getInfo(photo_id=f['id'], format='parsed-json')
        df_tags = pd.DataFrame(i1['photo']['tags']['tag'])
        df_dates = df_dates.append(pd.DataFrame.from_dict(i1['photo']['dates'], orient="index").T)
        #df_dates['photoid'] = theid
        #df_dates = normalize(i1['photo'],['dates'])
        #df_comments = pd.DataFrame(i1['photo']['comments'])
        df_comments = df_comments.append(pd.DataFrame.from_dict(i1['photo']['comments'], orient="index").T)

    print(df_dates.shape)
    pd.concat([df2, df_dates], axis=1)
    #df2 = df2.join(df_dates.shape)


def get_photo_batch(fobj, userid, page_to_get):
    # photos = fobj.photos.search(user_id=userid, per_page='30', extras='views')
    photos = flickr2.people.getPhotos(user_id=myuserid, page=page_to_get)

    df2 = pd.DataFrame(photos['photos']['photo'])
    dates = normalize(photos['photos']['photo'],'dates')

    calldb(df2,'photo_details')
    for f in photos['photos']['photo']:
            photoid = f['id']
            phototitle = f['title']

            faves = flickr2.photos.getFavorites(photo_id=photoid, per_page=50)
            df_faves = pd.DataFrame(faves['photo']['person'])
            id_list = [photoid for i in range(df_faves.index.size)]
            df_faves['photo_id'] = id_list
            calldb(df_faves, 'photo_faves')
            #countfaves = faves['photo']['total']

            i1 = flickr2.photos.getInfo(photo_id=photoid, format='parsed-json')
            df_tags = pd.DataFrame(i1['photo']['tags']['tag'])
            #photoids = ['photoid'] * range(df_tags.index.size)
            photoids = [photoid for i in range(df_tags.index.size)]
            df_tags['photo_id'] = photoids
            calldb(df_tags, 'photo_tags')

            tags = pd.json_normalize(i1['photo']['tags']['tag'])
            # dates = pd.json_normalize(info['photo']['dates'])
            # dates.to_json("./"+photoid+"dates.json")
            # tags.to_json("./"+photoid+"tags.json")
    print('Done with batch.')
    time.sleep(1)
    return (photos['photos']['page'], photos['photos']['pages'])

def get_all_favs(fobj, photoid):
    faves = fobj.photos.getFavorites(photo_id=photoid, per_page=50)
    countfaves = faves['photo']['total']
    df_faves = pd.DataFrame(faves['photo']['person'])
    while faves['photo']['pages']-faves['photo']['page'] > 0:
        faves = fobj.photos.getFavorites(photo_id=photoid, per_page=50, page=faves['photo']['page']+1)
        df_next = pd.DataFrame(faves['photo']['person'])
        df_faves = df_faves.append(df_next)
    id_list = [photoid for i in range(df_faves.index.size)]
    df_faves['photo_id'] = id_list


    return (countfaves, df_faves)
    #df_faves = pd.DataFrame(faves['photo']['person'])

def get_all():
    r = get_photo_batch(flickr2, myuserid, 1)
    while r[0] < r[1]:
        r = get_photo_batch(flickr2, myuserid, r[0] + 1)

    print('Done.')

def get_stats(datelist, last_livedate):
    #datelist = pd.date_range(date.today() - timedelta(29), periods=30).tolist()
    #delete from lastdate onward
    delete_stats_from_date(last_livedate)
    for d in datelist:
        ts = d.value
        dt = datetime.datetime(d.year, d.month, d.day)
        res = stat_helper(dt, 1)
        final_df = res[0]

        while res[1] - res[2] > 0:
            res = stat_helper(dt, res[2]+1)
            final_df = final_df.append(res[0])

        dt_list = [dt for i in range(final_df.index.size)]
        final_df['statdate'] = dt_list

        ### TODO this nees to be a single tranaction!
        calldb(final_df, 'photo_stats')
        mark_date_complete(d)
        print('nxt date')

    print('Done')

def stat_helper(dt,pg):
    popular = flickr2.stats.getPopularPhotos(date=dt, per_page=100, page=pg)
    df_popular = pd.DataFrame(popular['photos']['photo'])
    df_final = df_popular[['id', 'title']]
    df_stats = pd.DataFrame()
    for x in popular['photos']['photo']:
        df_stats = df_stats.append(x['stats'], ignore_index=1)
    df_final = df_final.join(df_stats)
    return df_final, popular['photos']['pages'], popular['photos']['page']

def domains_helper(dt,pg):
    popular = flickr2.stats.getPhotoDomains(date=dt, per_page=100, page=pg)
    df_domains = pd.DataFrame(popular['domains']['domain'])
    final_outer = pd.DataFrame()
    for dom in popular['domains']['domain']:
        refs = flickr2.stats.getPhotoReferrers(date=dt, domain=dom['name'], per_page=100, page=1)
        df_refs = pd.DataFrame(refs['domain']['referrer'])
        dt_domain = [dom['name'] for i in range(df_refs.index.size)]
        df_refs['domain'] = dt_domain
        final_df = df_refs
        while refs['domain']['pages'] - refs['domain']['page'] > 0:
            refs = flickr2.stats.getPhotoReferrers(date=dt, domain=dom['name'], per_page=100, page=refs['domain']['page']+1)
            df_refs = pd.DataFrame(refs['domain']['referrer'])
            dt_domain = [dom['name'] for i in range(df_refs.index.size)]
            df_refs['domain'] = dt_domain
            final_df = final_df.append(df_refs)

        final_outer = final_outer.append(final_df)

    return final_outer, popular['domains']['pages'], popular['domains']['page']

def get_domains(datelist, last_livedate):

    for d in datelist:
        res = domains_helper(d, 1)
        final_df = res[0]

        while res[1] - res[2] > 0:
            res = domains_helper(d, res[2]+1)
            final_df = final_df.append(res[0])

        dt_list = [d for i in range(final_df.index.size)]
        final_df['statdate'] = dt_list

        ### TODO this nees to be a single tranaction!
        calldb(final_df, 'photo_stats_domains')
        #mark_date_complete(d)
        print('nxt date')

    print('Done')



def ttest():
    #greekpeaks = flickr2.photos.getInfo(photo_id=49489963497, format='parsed-json')
    #photos = flickr2.people.getPhotos(user_id=myuserid, page=1)
    #df2 = pd.DataFrame(photos['photos']['photo'])


    connecting_string = 'Driver={ODBC Driver 17 for SQL Server};Server=tcp:woo.database.windows.net,1433;Database=BYWS;Uid=boss;Pwd=s7#3QzOsB$J*^v3;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    params = parse.quote_plus(connecting_string)

    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    connection = engine.connect()
    query = "SELECT * FROM dbo.photo_details"
    # query = "select max(statdate)  as stats_date,stats_state from dbo.stats_status group by stats_state union select  cast(getdate() as date) as  statdate, 'today' as stats_state"

    df = pd.read_sql(query, engine)  # Finally, importing the data into DataFrame df
    return df

def tests():
    #sets = flickr2.photosets.getList(user_id='58051209@N00')
    photos = flickr2.people.getPhotos(user_id=myuserid, page=1)
    df2 = pd.DataFrame(photos['photos']['photo'])
    # calldb(df2,'xxx')
    dt = datetime.datetime(2020, 2, 10, 7, 0)
    x = time.mktime(dt.timetuple())

    min_date = flickr2.photos.recentlyUpdated(min_date='2020-02-16 08:00:00')
    # try:
    #    popular = flickr2.stats.getPopularPhotos(date='2019-02-15', per_page=100)
    # except flickrapi.exceptions.FlickrError:
    #    print(flickrapi.exceptions.FlickrError)
    # allfavs = get_all_favs(flickr2,  30507209051)

    #'1581440149'
    # zz = get_saved_favs(30507209051)
    for f in photos['photos']['photo']:
        photoid = f['id']

        phototitle = f['title']
        i1 = flickr2.photos.getInfo(photo_id=photoid, format='parsed-json')
        exif = flickr2.photos.getExif(photo_id=photoid)
        groups = flickr2.photos.getAllContexts(photo_id=photoid)
        stats = flickr2.stats.getTotalViews(date='2020-01-31')

    # getStats()

    # info = flickr2.photos.getInfo(photo_id=f'49506034036', format='parsed-json')

    # set = flickr2.walk_set('72157703860493334', per_page=20)
    # for photo in set[1:4]:
    #     print ('Hello')

def normalize(ls, normalize_attr):
    df_norm = pd.DataFrame()
    for x in ls:
        df_norm = df_norm.append(x[normalize_attr], ignore_index=1)
    return df_norm


def call_func(funcname, top, it, retain_cols, normalize_attr, **kwargs):
    rs = funcname(**kwargs)
    df_rs = pd.DataFrame(rs[top][it])
    df_stats = normalize(rs[top][it], normalize_attr)

    while rs[top]['pages'] - rs[top]['page'] > 0:
        rs = funcname(**kwargs, page=rs[top]['page']+1)
        df_rs = pd.DataFrame(rs[top][it]).append(df_rs)
        df_stats = df_stats.append(normalize(rs[top][it], normalize_attr))

    df_final = df_rs[retain_cols]
    df_final = df_final.join(df_stats)

    return df_final



### These need to be moved to the future flickrana class
api_key = u'4a69280a31fc96c26e7d218c3a8cf345'
api_secret = u'a2d9a639fd955497'
myuserid = u'58051209@N00'

#api_key2 = u'ec9da859df80dec922bac6c9104c1f0f'
#api_secret2 = u'ee4603144f00eba9'


flickr2 = flickrapi.FlickrAPI(api_key, api_secret, format='parsed-json')
print('Step 1: authenticate')

# Only do this if we don't have a valid token already
if not flickr2.token_valid(perms='read'):

    # Get a request token
    flickr2.get_request_token(oauth_callback='oob')

    # Open a browser at the authentication URL. Do this however
    # you want, as long as the user visits that URL.
    authorize_url = flickr2.auth_url(perms='read')
    print(authorize_url)
    webbrowser.open_new_tab(authorize_url)

    # Get the verifier code from the user. Do this however you
    # want, as long as the user gives the application the code.
    verifier = str(input('Verifier code: '))


    # Trade the request token for an access token
    flickr2.get_access_token(verifier)

# TODO: from getInf
#       dates,comments views
#       save date of lst update get recentlyUpdated with photos.recentlyUpdated
#       * oAuth for stats
#       * more than 50 likes limit
#       groups from getAllContexts
#       * recentlyUpdate also need oAuth

start = time.time()
#dt = date.today()
#res = call_func(flickr2.stats.getPopularPhotos, 'photos', 'photo', ['id', 'title'], 'stats', per_page=100, date=dt)

#refresh_stats()
#dlist = get_saved_stats()
#get_domains(dlist, dlist[0])

test_photos(flickr2,myuserid)
print(f'Time: {time.time() - start}')

#get_saved_favs(8889)
print('Done.')





