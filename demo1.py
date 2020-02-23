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
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
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
        # print(r1)
        trans.commit()
    except:
        trans.rollback()
        raise
    finally:
        connection.close()

def _delete_stats_from_date(dt):
    name = 'stats_photos'

    connecting_string = 'Driver={ODBC Driver 17 for SQL Server};Server=tcp:woo.database.windows.net,1433;Database=BYWS;Uid=boss;Pwd=s7#3QzOsB$J*^v3;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    params = parse.quote_plus(connecting_string)

    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    query = """
                    IF OBJECT_ID('dbo.{}') IS NOT NULL 
                    delete from dbo.{} where statdate = CAST( ? AS DATE) AND userid = ?; 
                    """.format(name, name)
    #
    connection = engine.connect()
    params = (dt, myuserid)

    trans = connection.begin()
    try:
        print(query)
        res = connection.execute(query, params)
        print(res.rowcount)
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

    query = "insert into dbo.stats_status (stats_date,stats_state)\
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
    query = "select max(stats_date)  as stats_date,stats_state from dbo.stats_status group by stats_state \
            union select  cast(getdate() as date) as  statdate, 'today' as stats_state"

    #union select  '2020-02-14' as  statdate, 'live' as stats_state\
    df = pd.read_sql_query(query, engine)  # Finally, importing the data into DataFrame df
    livedate = df[df.stats_state.eq('live')]['stats_date'][1]

    #query = "delete from dbo.photo_stats where statdate >= {}".format(livedate.date())
    #delete_stats_from_date(livedate.date())

    foo = df[df.stats_state.eq('frozen')]['stats_date']
    t = date.today()
    bar = pd.date_range(start=livedate, end=date.today()).tolist()

    return bar
    # ndf = df[df['favedate'] > str(1498608604)]


def get_photo_stats(dt):
    popular = flickr2.stats.getPopularPhotos(date=dt, per_page=100, page=0)
    df_popular = pd.DataFrame(popular['photos']['photo'])

    while popular['photos']['pages'] - popular['photos']['page'] > 0:
        popular = flickr2.stats.getPopularPhotos(date=dt, per_page=100, page=popular['photos']['page']+1)
        df_popular = df_popular.append(pd.DataFrame(popular['photos']['photo']))

    df_popular = df_popular[['id', 'title', 'stats']]
    df_stats = pd.DataFrame(df_popular['stats'].values.tolist())
    #df_popular.drop('stats', axis=1, inplace=True)
    #df_popular = df_popular.merge(df_stats,on='id')

    df_popular['views'] = df_stats['views']
    df_popular['favorites'] = df_stats['favorites']
    df_popular['comments'] = df_stats['comments']
    df_popular['total_views'] = df_stats['total_views']
    df_popular['total_favorites'] = df_stats['total_favorites']
    df_popular['total_comments'] = df_stats['total_comments']
    df_popular.drop('stats', axis=1, inplace=True)
    dt_list = [dt for i in range(df_popular.index.size)]
    df_popular['statdate'] = dt_list

    photo_domain_stats = _get_domains(flickr2.stats.getPhotoDomains, flickr2.stats.getPhotoReferrers, pd.date_range(dt, periods=1).tolist(), None)
    print(df_popular)
    return (df_popular, photo_domain_stats )

def get_set_stats(dt):

    photosets = flickr2.photosets.getList(user_id=myuserid, per_page=100, page=1)
    df_sets = pd.DataFrame(photosets['photosets']['photoset'])
    while photosets['photosets']['pages'] - photosets['photosets']['page'] > 0:
        photosets = flickr2.people.photosetsGetList(user_id=myuserid, per_page=100, page=photosets['photossets']['page']+1)
        df_sets = df_sets.append(pd.DataFrame(photosets['photosets']['photoset']))

    ## drop unecessary columns
    df_sets = df_sets.drop(['secret', 'server', 'farm', 'primary'], 1)

    ## copy out description and title to normalize
    titles = [i['_content'] for i in df_sets['title'].values]
    descriptions = [i['_content'] for i in df_sets['description'].values]
    df_sets = df_sets.drop(['title', 'description'], 1)
    df_sets['title'] = pd.Series(titles)
    df_sets['description'] = pd.Series(descriptions)

    df_setstats = pd.DataFrame()
    for setid in df_sets['id']:
        ss = flickr2.stats.getPhotosetStats(photoset_id=setid, date=dt)
        df_setstats = df_setstats.append(pd.DataFrame([ {'date': dt, 'views': ss['stats']['views'], 'comments': ss['stats']['comments']}]))

    sets_domain_stats = _get_domains(flickr2.stats.getPhotosetDomains, flickr2.stats.getPhotosetReferrers, pd.date_range(dt, periods=1).tolist(), None)
    #df_sets['views'] = df_setstats['views']
    #df_sets['comments'] = df_setstats['comments']
    #df_sets["id"] = df_sets["id"].astype('int64')
    #df_setstats["id"] = df_setstats["id"].astype('int64')
    df_sets = df_sets.join(df_setstats.reset_index(drop=True))
    print(df_setstats)
    return (df_sets, sets_domain_stats)

def parse_col_tree(colid,df_cols):
    col_root = flickr2.collections.getTree(user_id=myuserid, collection_id=colid)
    df_cols = df_cols.append(pd.DataFrame(col_root['collections']['collection']))

    for col in col_root['collections']['collection']:
        if 'collection' in col:
            for cn in col['collection']:
                df_cols = parse_col_tree(cn['id'], df_cols)

    return df_cols

def get_collection_stats(dt):
    df_cols = pd.DataFrame()
    df_cols = parse_col_tree(0, df_cols)

    ## drop unecessary columns
    df_cols = df_cols[['id', 'title']]
    df_colstats = pd.DataFrame()

    for colid in df_cols['id']:
        try:
            realid = colid[colid.find('-')+1:]
            cs = flickr2.stats.getCollectionStats(collection_id=realid, date=dt)
        except flickrapi.exceptions.FlickrError:
            print(flickrapi.exceptions.FlickrError)
        df_colstats = df_colstats.append(pd.DataFrame([{'date': dt, 'id': colid, 'views': cs['stats']['views'],
                                                        'title': df_cols.loc[df_cols['id'] == colid]['title'][0]}]))

    cols_domain_stats = _get_domains(flickr2.stats.getCollectionDomains, flickr2.stats.getCollectionReferrers, pd.date_range(dt, periods=1).tolist(), None)
    print(df_colstats)
    return (df_colstats, cols_domain_stats)

def get_stream_stats(dt):

    stream = flickr2.stats.getPhotostreamStats(date=dt)
    df_streams = pd.DataFrame([{ 'date': dt, 'views': stream['stats']['views']}])

    stream_domain_stats = _get_domains(flickr2.stats.getPhotostreamDomains, flickr2.stats.getPhotostreamReferrers, pd.date_range(dt, periods=1).tolist(), None)
    print(stream_domain_stats)
    return (df_streams, stream_domain_stats)

def get_totals_stats(dt):
    totals = flickr2.stats.getTotalViews(date=dt)
    df_totals = pd.DataFrame([{'date': dt,
                                    'total': totals['stats']['total']['views'],
                                    'photos': totals['stats']['photos']['views'],
                                    'photostream': totals['stats']['photostream']['views'],
                                    'sets': totals['stats']['sets']['views'],
                                    'galleries': totals['stats']['galleries']['views'],
                                    'collections': totals['stats']['collections']['views']
                                }])
    print(df_totals)
    return df_totals

def get_all_stats(dt):
    writelst = []

    df_photo_stats = get_photo_stats(dt)
    df_photo_stats[0].name = 'stats_photos'
    df_photo_stats[1].name = 'stats_photos_domains'
    writelst.append(df_photo_stats[0])
    writelst.append(df_photo_stats[1])

    df_totals = get_totals_stats(dt)
    df_totals.name = 'stats_totals'
    writelst.append(df_totals)

    df_streams = get_stream_stats(dt)
    df_streams[0].name = 'stats_streams'
    df_streams[1].name = 'stats_stream_domains'
    writelst.append(df_streams[0])
    writelst.append(df_streams[1])

    df_sets = get_set_stats(dt)
    df_sets[0].name = 'stats_sets'
    df_sets[1].name = 'stats_sets_domains'
    writelst.append(df_sets[0])
    writelst.append(df_sets[1])

    df_cols = get_collection_stats(dt)
    df_cols[0].name = 'stats_collections'
    df_cols[1].name = 'stats_collections_domains'
    writelst.append(df_cols[0])
    writelst.append(df_cols[1])


    for i in writelst:
        write_df(dt, i)

    print('retrieved all stats')

def write_df(dt, i):

    if i.shape[0] > 0:

        connecting_string = 'Driver={ODBC Driver 17 for SQL Server};Server=tcp:woo.database.windows.net,1433;Database=BYWS;Uid=boss;Pwd=s7#3QzOsB$J*^v3;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
        params = parse.quote_plus(connecting_string)

        engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)
        connection = engine.connect()

        # session = sessionmaker(bind=engine)

        # new session.   no connections are in use.
        #session = session()
        trans = connection.begin()
        try:

            query = """
                    IF OBJECT_ID('dbo.{}') IS NOT NULL 
                    delete from dbo.{} where statdate = CAST( ? AS DATE) AND userid = ?; 
                    """.format(i.name, i.name)
            #connection = engine.connect()
            params = ( dt, myuserid )
            print(query)
            res = connection.execute(query, params )
            print(res.rowcount)
            dt_list = [dt for i in range(i.index.size)]
            user_list = [myuserid for i in range(i.index.size)]
            i['statdate'] = dt_list
            i['statdate'] = pd.to_datetime(i['statdate'])
            i['userid'] = user_list
            print(i.shape[0])
            i.to_sql(i.name, con=engine, if_exists='append', chunksize=1000)

            trans.commit()

        except:
            trans.rollback()
            raise
        finally:
            connection.close()

def test_photos(fobj, myuserid):

    photos = flickr2.people.getPhotos(user_id=myuserid, per_page=100, page=1)

    df2 = pd.DataFrame(photos['photos']['photo'])
    df_dates = pd.DataFrame()
    df_url = pd.DataFrame()
    for f in photos['photos']['photo']:
        photoid = f['id']
        phototitle = f['title']


        i1 = flickr2.photos.getInfo(photo_id=photoid, format='parsed-json')
        df_tags = pd.DataFrame(i1['photo']['tags']['tag'])
        #photoids = ['photoid'] * range(df_tags.index.size)
        photoids = [photoid for i in range(df_tags.index.size)]
        df_tags['photo_id'] = photoids

        # df_dates = pd.DataFrame(i1['photo'])['dates'] # ['posted']
        # pd.DataFrame(i1[('id','photo')])['dates']  # ['posted']
        dfx = pd.DataFrame(i1['photo'])
        df_dates = df_dates.append(dfx.loc[['lastupdate'], ['id', 'dates']].append(dfx.loc[['posted', 'taken'], ['id', 'dates']]))
        # df_alldates = df_alldates.append(df_dates)
        # df_url = pd.DataFrame(i1['photo'])['urls']['url']
        df_url = df_url.append(pd.DataFrame(pd.DataFrame(i1['photo'])['urls']['url']))
        # df_allurls = df_allurls.append(df_url)

        #df_dates = df_dates.append(pd.DataFrame.from_dict(i1['photo']['dates'], orient="index").T)
        #df_dates['photoid'] = theid
        #df_dates = normalize(i1['photo'],['dates'])
        #df_comments = pd.DataFrame(i1['photo']['comments'])
        #df_comments = df_comments.append(pd.DataFrame.from_dict(i1['photo']['comments'], orient="index").T)

    print(df_dates.shape)
    # pd.concat([df2, df_dates], axis=1)
    # df2 = df2.join(df_dates.shape)


def get_photo_batch(fobj, userid, page_to_get):
    # photos = fobj.photos.search(user_id=userid, per_page='30', extras='views')
    photos = flickr2.people.getPhotos(user_id=myuserid, page=page_to_get)

    df2 = pd.DataFrame(photos['photos']['photo'])
    #dates = normalize(photos['photos']['photo'],'dates')

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


def _domains_helper(dom_func, referrers_func, dt, pg):
    popular = dom_func(date=dt, per_page=100, page=pg)
    final_outer = pd.DataFrame()
    if int( popular['domains']['pages']) > 0:
        df_domains = pd.DataFrame(popular['domains']['domain'])

        for dom in popular['domains']['domain']:
            refs = referrers_func(date=dt, domain=dom['name'], per_page=100, page=1)
            df_refs = pd.DataFrame(refs['domain']['referrer'])
            dt_domain = [dom['name'] for i in range(df_refs.index.size)]
            df_refs['domain'] = dt_domain
            final_df = df_refs
            while refs['domain']['pages'] - refs['domain']['page'] > 0:
                refs = referrers_func(date=dt, domain=dom['name'], per_page=100, page=refs['domain']['page']+1)
                df_refs = pd.DataFrame(refs['domain']['referrer'])
                dt_domain = [dom['name'] for i in range(df_refs.index.size)]
                df_refs['domain'] = dt_domain
                final_df = final_df.append(df_refs)

            final_outer = final_outer.append(final_df)

    return final_outer, popular['domains']['pages'], popular['domains']['page']

def _get_domains(dom_func, referrers_func, datelist, last_livedate):
    for d in datelist:
        res = _domains_helper(dom_func, referrers_func, d, 1)
        final_df = res[0]

        while res[1] - res[2] > 0:
            res = _domains_helper(dom_func, referrers_func, d, res[2] + 1)
            final_df = final_df.append(res[0])

        dt_list = [d for i in range(final_df.index.size)]
        final_df['statdate'] = dt_list
        return final_df


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



# datelist = pd.date_range(date.today() - timedelta(1), periods=1).tolist()

# getPhotoStats or getPopularPhotos
# df = _get_domains(flickr2.stats.getPhotoDomains, flickr2.stats.getPhotoReferrers, datelist, date.today())

# l = flickr.photosets.getList user_id
# for all in l
#    getPhotosetStats
# df1 = _get_domains(flickr2.stats.getPhotosetDomains, flickr2.stats.getPhotosetReferrers, datelist, date.today())

# next = flickr.collections.getTree(0)
#    if flickr.collections.getTree(next)..:
# df2 = _get_domains(flickr2.stats.getCollectionDomains, flickr2.stats.getCollectionReferrers, datelist, date.today())


#get_all_stats('2020-02-21')

# get_collection_stats('2020-02-23')
get_all_stats('2020-02-23')
#_delete_stats_from_date('2020-02-19')

#refresh_stats()

#dlist = get_saved_stats()
#get_domains(dlist, dlist[0])

# test_photos(flickr2,myuserid)
print(f'Time: {time.time() - start}')

#get_saved_favs(8889)
print('Done.')





