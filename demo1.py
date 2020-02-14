import time
from urllib import parse

import flickrapi
# from pandas.io.json import json_normalize
import pandas as pd
import sqlalchemy as sa
import webbrowser
import datetime
from datetime import date, timedelta
import time



def calldb(data,table_name):

    connecting_string = 'Driver={ODBC Driver 17 for SQL Server};Server=tcp:woo.database.windows.net,1433;Database=BYWS;Uid=boss;Pwd=s7#3QzOsB$J*^v3;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    params = parse.quote_plus(connecting_string)

    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    connection = engine.connect()
    # result = connection.execute(strSelect)
    data.to_sql(table_name, con=engine, if_exists='append', chunksize=1000)

    # f = pd.read_sql(strSelect, engine)

    # connection.close()

def get_saved_favs(pid):
    connecting_string = 'Driver={ODBC Driver 17 for SQL Server};Server=tcp:woo.database.windows.net,1433;Database=BYWS;Uid=boss;Pwd=s7#3QzOsB$J*^v3;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    params = parse.quote_plus(connecting_string)

    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    connection = engine.connect()
    query = "SELECT * FROM dbo.photo_faves WHERE photo_id = '" + str(pid) + "'"
    df = pd.read_sql_query(query, engine)  # Finally, importing the data into DataFrame df

    ndf = df[df['favedate'] > str(1498608604)]


def get_photo_batch(fobj,userid,page_to_get):
    # photos = fobj.photos.search(user_id=userid, per_page='30', extras='views')
    photos = flickr2.people.getPhotos(user_id=myuserid, page=page_to_get)

    df2 = pd.DataFrame(photos['photos']['photo'])
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
    try:
        # fname = "./"+photoid+"favs.json"
        with open('x.json', 'w') as jf:
            jf.write(df_faves.to_json(orient='records', lines=True))
    except:
        print("Error.")

    return (countfaves, df_faves)
    #df_faves = pd.DataFrame(faves['photo']['person'])

def get_all():
        r = get_photo_batch(flickr2, myuserid, 1)
        while r[0] < r[1]:
                r = get_photo_batch(flickr2, myuserid, r[0] + 1)

        print('Done.')

def stat_helper(dt,pg):
    popular = flickr2.stats.getPopularPhotos(date=dt, per_page=100, page=pg)
    df_popular = pd.DataFrame(popular['photos']['photo'])
    df_final = df_popular[['id', 'title']]
    df_stats = pd.DataFrame()
    for x in popular['photos']['photo']:
        df_stats = df_stats.append(x['stats'], ignore_index=1)
    df_final = df_final.join(df_stats)
    return ( df_final, popular['photos']['pages'] , popular['photos']['page'])

def getStats():
    #dt =
    datelist = pd.date_range(date.today() - timedelta(29), periods=30).tolist()
    for d in datelist:
        ts = d.value
        dt = datetime.datetime(d.year, d.month, d.day)
        res = stat_helper(dt,1)
        final_df = res[0]
        # popular = flickr2.stats.getPopularPhotos(date=dt, per_page=100, page=1)
        # df_popular = pd.DataFrame(popular['photos']['photo'])
        # df_id = df_popular[['id', 'title']]
        # #df_stats = pd.DataFrame(df_popular['stats'])
        # #df_s = pd.DataFrame(df_stats['stats'].values)
        # fd_# f = pd.DataFrame()
        # for x in popular['photos']['photo']:
        #     fd_f = fd_f.append(x['stats'], ignore_index=1)
        # df_id = df_id.join(fd_f)
        while res[1] - res[2] > 0:
            res = stat_helper(dt, res[2]+1)
            final_df = final_df.append(res[0])

        # while popular['photos']['pages'] - popular['photos']['page'] > 0:
        #     popular = flickr2.stats.getPopularPhotos(date=dt, per_page=100, page=popular['photos']['page'] + 1)
        #     df_popular = pd.DataFrame(popular['photos']['photo'])
        #     # df_id = df_popular[['id', 'title']]
        #     for x in popular['photos']['photo']:
        #         fd_f = fd_f.append(x['stats'], ignore_index=1)
        #     df_id = df_id.join(fd_f)
        #     df_id = df_id.append(df_next)

        dt_list = [dt for i in range(final_df.index.size)]
        final_df['statdate'] = dt_list
        # write off to db
        calldb(final_df, 'photo_stats')
        print('nxt date')

    print('Done')

api_key = u'4a69280a31fc96c26e7d218c3a8cf345'
api_secret = u'a2d9a639fd955497'

api_key2 = u'ec9da859df80dec922bac6c9104c1f0f'
api_secret2 = u'ee4603144f00eba9'
myuserid = u'58051209@N00'

flickr2 = flickrapi.FlickrAPI(api_key, api_secret, format='parsed-json')
print('Step 1: authenticate')

# Only do this if we don't have a valid token already
if not flickr2.token_valid(perms='read'):

    # Get a request token
    flickr2.get_request_token(oauth_callback='oob')

    # Open a browser at the authentication URL. Do this however
    # you want, as long as the user visits that URL.
    authorize_url = flickr2.auth_url(perms='read')
    webbrowser.open_new_tab(authorize_url)

    # Get the verifier code from the user. Do this however you
    # want, as long as the user gives the application the code.
    verifier = str(input('Verifier code: '))


    # Trade the request token for an access token
    flickr2.get_access_token(verifier)





# flickr = flickrapi.FlickrAPI(api_key2, api_secret2)
# flickr2 = flickrapi.FlickrAPI(api_key, api_secret, format='etree')



# raw_json = flickr2.photosets.getList(user_id='58051209@N00')
# raw_json -> '{...}'
#
#res = flickr2.people.getPhotos(user_id=myuserid)
# parsed = json.loads(raw_json.decode('utf-8'))

sets = flickr2.photosets.getList(user_id='58051209@N00')
photos = flickr2.people.getPhotos(user_id=myuserid, page=1)
#dt = datetime.datetime(2020, 2, 14, 10, 00)
#x = time.mktime(dt.timetuple())
#min_date = flickr2.photos.recentlyUpdated(min_date=x)
#try:
#    popular = flickr2.stats.getPopularPhotos(date='2019-02-13', per_page=100)
#except flickrapi.exceptions.FlickrError:
#    print(flickrapi.exceptions.FlickrError)
#allfavs = get_all_favs(flickr2,  30507209051)


#zz = get_saved_favs(30507209051)
getStats()

# TODO: from getInf
#       dates,comments views
#       save date of lst update get recentlyUpdated with photos.recentlyUpdated
#       * oAuth for stats
#       * more than 50 likes limit
#       groups from getAllContexts
#       * recentlyUpdate also need oAuth

for f in photos['photos']['photo']:
    photoid = f['id']

    phototitle = f['title']
    i1 = flickr2.photos.getInfo(photo_id=photoid, format='parsed-json')
    exif = flickr2.photos.getExif(photo_id=photoid)
    groups = flickr2.photos.getAllContexts(photo_id=photoid)
    stats = flickr2.stats.getTotalViews(date='2020-01-31')
print ('Done.')

# infojson  = json.load(info)
# with open('T:/@dev/python/flickr/photoinfo.json', 'w') as outfile:
#    json.dump(infojson, outfile)

# info = flickr2.photos.getInfo(photo_id=f'49506034036', format='parsed-json')




# set = flickr2.walk_set('72157703860493334', per_page=20)
# for photo in set[1:4]:
#     print ('Hello')




