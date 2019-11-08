# Korey O'Dell
#
#
from os import fork, getpid
import os
import os.path
import subprocess
import json
import sys
import time
import calendar, datetime
from datetime import timedelta
import select
import glob
import pymysql
#import MySQLdb.cursors
from collections import defaultdict
import warnings
import requests
#warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore")
# some constants

_DEVEL = 0
_LAB = 1
_PROD = 2
_RUN_MODE = _DEVEL


_DB_HOST = ""
_DB_USER = ""
_DB_PASSWD = ""
_DB_DATABASE = ""

_DEV_DB_HOST = "localhost"
_DEV_DB_USER = "converter"
_DEV_DB_PASSWD = "********" # not fond of these creds here
_DEV_DB_DATABASE = "converter_dev"
_DEV_CONVERTER_DEST_PATH = "test"

_LAB_DB_HOST = "localhost"
_LAB_DB_USER = "converter"
_LAB_DB_PASSWD = "*****" # not fond of these creds here
_LAB_DB_DATABASE = "converter_lab"

_PROD_DB_HOST = "localhost"
_PROD_DB_USER = "converter"
_PROD_DB_PASSWD = "**********" # not fond of these creds here
_PROD_DB_DATABASE = "converter_prod"
_PROD_CONVERTER_DEST_PATH = "/mnt/nj1-prdev-sv01/hscd96/transfer_da"

_DEBUG=0
_INFO=1
_NOTIFY=2
_WARNING=4
_ERROR=8
_FATAL=16
_LOG_LEVEL=_INFO
_LEVELS = [0] * 20
_LEVELS[_DEBUG] = "DEBUG"
_LEVELS[_INFO] = "INFO"
_LEVELS[_NOTIFY] = "NOTIFY"
_LEVELS[_WARNING] = "WARNING"
_LEVELS[_ERROR] = "ERROR"
_LEVELS[_FATAL] = "FATAL"

_READY=0
_STAGED=1
_PROCESSING=2
_COMPLETE=16
_FAILED=32
_STATES = [0] * 20
_STATES[_PROCESSING] = "PROCESSING"
_STATES[_COMPLETE] = "COMPLETE"
_FFMPEG = "/usr/bin/ffmpeg"
_RSYNC = "/usr/bin/rsync"
module="converter"
run_pid_dir = "/var/run/DeepAd"
log_dir = "logs"
data_dir = "tmp"
_MAX_WORKERS=1


        
def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())

def log( msg, level=_INFO ):
    if level >= _LOG_LEVEL:
        print("** {} {} {} '{}'".format( module, time.asctime( time.localtime(time.time())), _LEVELS[level], msg ) )
        f = open("{}/{}.log".format( log_dir, module ), "a")
        f.write("** {} {} {} '{}'\n".format( module, time.asctime( time.localtime(time.time())), _LEVELS[level], msg ) )
        f.close
    
def usage():
    print("****************************************************************")
    print("**                                                            **")
    print("** Converter                                                  **")
    print("**                                                            **")
    print("Usage: {} dev|lab|prod".format( sys.argv[0] ) )
    print("**                                                            **")
    print("****************************************************************")

if os.path.exists( log_dir ) is False:
    os.makedirs( log_dir )

if os.path.exists( data_dir ) is False:
    os.makedirs( data_dir )

if os.path.exists( run_pid_dir ) is False:
    log( "run_pid directory '{}' does not exist. Please create and ensure it is writeable by the user running this script.".format( run_pid_dir ), _FATAL )
    exit(1)
# see if we are running already
c_pid = os.getpid()
pidfh = None
if os.path.exists( "{}/{}".format( run_pid_dir, module ) ) == True:
    pidfh = open( "{}/{}".format( run_pid_dir, module ), 'r' )
    if pidfh:
        r_pid = pidfh.read()
        if r_pid:
            alive = os.system( "kill -0 {} 2>/dev/null".format( r_pid ) )
            if alive == 0: # running
                log( "already running", _INFO )
                exit(0)

# let it fall into run loop below
if pidfh:
    pidfh.close()
pidfh = open( "{}/{}".format( run_pid_dir, module ), 'w' )
pidfh.write( "{}".format( c_pid) )
pidfh.close()

if( len(sys.argv) < 2 ):
    usage()
    exit(1)

_DB_HOST = ""
_DB_USER = ""
_DB_PASSWD = ""
_DB_DATABASE = ""
    
if sys.argv[1].lower() == "lab":
    running_mode = "lab"
    _DB_HOST = _LAB_DB_HOST
    _DB_USER = _LAB_DB_USER
    _DB_PASSWD = _LAB_DB_PASSWD
    _DB_DATABASE = _LAB_DB_DATABASE
elif sys.argv[1].lower() == "prod":
    running_mode = "prod"
    _DB_HOST = _PROD_DB_HOST
    _DB_USER = _PROD_DB_USER
    _DB_PASSWD = _PROD_DB_PASSWD
    _DB_DATABASE = _PROD_DB_DATABASE
    _CONVERTER_DEST_PATH = _PROD_CONVERTER_DEST_PATH    
else:
    running_mode = "dev"
    _DB_HOST = _DEV_DB_HOST
    _DB_USER = _DEV_DB_USER
    _DB_PASSWD = _DEV_DB_PASSWD
    _DB_DATABASE = _DEV_DB_DATABASE
    _CONVERTER_DEST_PATH = _DEV_CONVERTER_DEST_PATH
    
log( "running in '{}' mode".format( running_mode ) )
log( "db params host:{} user:{} name:{}".format( _DB_HOST, _DB_USER, _DB_DATABASE ) )
log( "converter dest path:{}".format( _CONVERTER_DEST_PATH ) )
log( "starting up : pid {}".format(c_pid))

# this is the local db, used for state, etc.
conn=pymysql.connect(
    host=_DB_HOST,
    user=_DB_USER,
    passwd=_DB_PASSWD,
    db=_DB_DATABASE,
    charset='utf8',
    cursorclass = pymysql.cursors.SSDictCursor)
c = conn.cursor()

oconn=pymysql.connect(
    host=_DB_HOST,
    user=_DB_USER,
    passwd=_DB_PASSWD,
    db=_DB_DATABASE,
    charset='utf8',
    cursorclass = pymysql.cursors.SSDictCursor)
oc = oconn.cursor()

num_workers = 0
processes = defaultdict( dict )

while 1:
    # only go back seven days?
    epoch = calendar.timegm( datetime.datetime.utcnow().utctimetuple() )
    #log( "current UTC epoch time: {}".format( epoch ))
    sql = "select * from schedule"
    res = oc.execute(sql)
    row = oc.fetchone()
    adj_time = 0
    next_time = 0
    while row is not None:

        adj_time = int(row['epoch_10m'])
        next_time = adj_time + 600

        if int(row['status']) == _READY:
            new_ids = []
            update_ids = []
            skipped_ids = []
            schedule_id = row['id']
            log( "processing epoch {} {}".format( row['epoch_10m'], row['epoch_dt'] ) )
            mydate = datetime.datetime.utcfromtimestamp( adj_time - 3600 )

            l_year = int(mydate.strftime("%Y"))
            l_month = int(mydate.strftime("%m"))
            l_mday = int(mydate.strftime("%d"))
            l_hour = int(mydate.strftime("%H"))
            l_minute = int(mydate.strftime("%M"))
            l_second = int(mydate.strftime("%S"))
        
            mydate = datetime.datetime.utcfromtimestamp( adj_time )
        
            n_year = int(mydate.strftime("%Y"))
            n_month = int(mydate.strftime("%m"))
            n_mday = int(mydate.strftime("%d"))
            n_hour = int(mydate.strftime("%H"))
            n_minute = int(mydate.strftime("%M"))
            n_second = int(mydate.strftime("%S"))
        
            l_year = "{:0>4}".format( l_year )
            l_month = "{:0>2}".format( l_month )
            l_mday = "{:0>2}".format( l_mday )
            l_hour = "{:0>2}".format( l_hour )
            l_minute = "{:0>2}".format( l_minute )
            l_second = "{:0>2}".format( 0 )
        
            n_year = "{:0>4}".format( n_year )
            n_month = "{:0>2}".format( n_month )
            n_mday = "{:0>2}".format( n_mday )
            n_hour = "{:0>2}".format( n_hour )
            n_minute = "{:0>2}".format( n_minute )
            n_second = "{:0>2}".format( 0 )

            time_span = "{}{}{}T{}{}{}Z..{}{}{}T{}{}{}Z".format(l_year, l_month, l_mday, l_hour, l_minute, l_second, n_year, n_month, n_mday, n_hour, n_minute, n_second)
            string_temp = "https://entry.prod.mt.deep.ad/api/posts/?query=rating%3Asafe%20frozen-time%3ATIMESPAN%20&limit=3"
            request_string = string_temp.replace('TIMESPAN', time_span)
            log( "requesting range {}-{} => {} from DeepAd".format( adj_time-3600, adj_time, time_span ))
            log(request_string)

            try:
                response = requests.get(request_string,  headers={'Accept' : 'application/json', 'authorization' : 'Basic ZXhwb3J0OmxrbTMwODRqaGE='}, timeout=60 )    
            except requests.exceptions.RequestException as e:
                log( "request failed {}".format( e ), _ERROR )

            data = response.json()
            #            log(data)
            # parse through JSON, insert new jobs and/or update existing jobs that have a new frozen time


            # have to check for id here and then if frozen-time == frozen-time
            for i in range(len(data['results'])):
                sql = "select distinct id, frozen_time from jobs where `id`='{}'".format( data['results'][i]['id'] )
                res = c.execute( sql )
                row = c.fetchone()
                if row is not None:
                    if data['results'][i]['frozenTime'] == row['frozen_time']:
                        skipped_ids.append( i) 
                        log( "already have asset id {} frozen_time {} == frozen_time {}".format ( data['results'][i]['id'], data['results'][i]['frozenTime'], row['frozen_time'] ), _INFO )
                    else:
                        log( "already have asset id {} but frozen_time {} != frozen_time {}".format ( data['results'][i]['id'], data['results'][i]['frozenTime'], row['frozen_time'] ), _INFO )
                        update_ids.append( i )
                else:
                    new_ids.append( i )
                        
            while row:
                row = c.fetchone()

            if update_ids:
                total_updated=0;
                for i in update_ids:
                    log( "updating asset id {} {}".format( data['results'][i]['id'], data['results'][i]['pattern'] ) )
                    sql = "update jobs set schedule='{}', status='{}', frozen_time='{}' where id='{}'".format( schedule_id, _READY, data['results'][i]['frozenTime'], data['results'][i]['id'])
                    res=c.execute( sql )
                    conn.commit()
                    total_updated += 1
                log( "updated {} ads in jobs table".format( total_updated) )
                
            if new_ids:
                total_inserted=0;
                for i in new_ids:
                    log( "inserting asset id {} {}".format( data['results'][i]['id'], data['results'][i]['pattern'] ) )
                    sql = "insert into jobs values ( '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}' )".format( data['results'][i]['id'], schedule_id, data['results'][i]['frozenTime'], data['results'][i]['mp4'], data['results'][i]['contentUrl'], data['results'][i]['pattern'], data['results'][i]['duration'], 0, 0, 0, int(time.time()) ) 
                    #log( sql, _DEBUG )
                    res=c.execute( sql )
                    conn.commit()
                    total_inserted += 1
                log( "inserted {} new ads into jobs table".format( total_inserted) )
            if skipped_ids:
                log( "skipped {} ads because they already have been processed".format( len( skipped_ids )) )
            log( "total assets in JSON response {} ".format( data['total'] ) )
            sql = "update schedule set total_assets = '{}', actual_assets = '{}', status='{}' where id='{}'".format( data['total'], data['total'] - len( skipped_ids ), _STAGED, schedule_id )
            res=c.execute( sql )
            conn.commit()
             
        row = oc.fetchone()
    if False and next_time < epoch: # now - insert another entry in schedule

        mydate = datetime.datetime.utcfromtimestamp( next_time )
        n_year = "{:0>4}".format( int(mydate.strftime("%Y")) )
        n_month = "{:0>2}".format( int(mydate.strftime("%m")) )
        n_mday = "{:0>2}".format( int(mydate.strftime("%d")) )
        n_hour = "{:0>2}".format( int(mydate.strftime("%H")) )
        n_minute = "{:0>2}".format( int(mydate.strftime("%M")) )
        n_second = "{:0>2}".format( int(mydate.strftime("%S")) )
        n_date = "{}/{}/{} @ {}:{} (UTC)".format( n_month, n_mday, n_year, n_hour, n_minute )
        log( "inserting line into schedule for {} / {}".format( next_time, n_date) )
        sql = "insert into schedule values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}' )".format( 0, next_time, n_date, 0, _READY, 0, 0, 0, "unix_timestamp()" )
        res=c.execute( sql )
        conn.commit()
    # here we will check for work and fork abundantly to get 'er done
    # the idea being we check the schedule table for _READY status
    # then select jobs on that sched_id
    # we'll make a another pass to catch the new freezes - or - have the freeze above change the status to
    # ready for that entry?
    


    # process asset

    sql = "select id from schedule where status='{}'".format( _STAGED )
    log( sql, _DEBUG )
    oc.execute( sql )
    row = oc.fetchone()
    if row is not None:
        sched_id = row['id']
        log( "we've got work to do kids..." )
        log( "processing schedule_id '{}'".format( sched_id ) )
        jobs_done = 0
        o_et = int(round(time.time() * 1000 ) )
        done = 0
        while done == 0:
            sql = "select id, mp4_url, content_url, pattern, status from jobs where schedule_id='{}' and status='{}'".format( sched_id, _READY )
            log( sql, _DEBUG )
            c.execute( sql )
            row = c.fetchone()
            if row:            
                if num_workers < _MAX_WORKERS:
                    id = row['id']
                    pid = os.fork()
                    if pid == 0:
                    # kid get a job, process it, rsync it, delete it, exit
                        creatives_list = []
                        job_id = row['id']
                        sql = "update jobs set status=%s where id=%s"
                        c.execute( sql, (_PROCESSING, job_id)  )
                        log( "worker processing asset id {}".format( job_id ) )
                        # todo check incoming asset for health
                        # ffmpeg it and rsync it, then delete it.
                        et = int(round(time.time() * 1000 ) )
                        sql = "update jobs set status=%s where id=%s"
                        log( sql, _DEBUG )
                        c.execute( sql, (_PROCESSING, job_id)  )
                        mp4_url = row['mp4_url']
                        content_url = row['content_url']
                        the_url = ""
                        log( "worker mp4 {} contenturl {}".format( mp4_url, content_url ) )

                        if mp4_url == "None":
                            log( "mp4 is none - using contenturl" )
                            the_url = content_url
                            src_file = "{}.mp4".format( row['pattern'] )
                        else:
                            the_url = mp4_url
                            src_file = "{}.mp4".format( row['pattern'] )

                            log( "src_file {}".format( src_file ) )

                        
                        # get tasks from db table tasks i.e. ffmpeg cmd params
                        sql = "select * from tasks"
                        log( sql, _DEBUG )
                        c.execute( sql )
                        row = c.fetchone()
                        failures = 0
                        while row is not None and failures == 0:
                            met = int(round(time.time() * 1000 ) )
                            if os.path.exists( row['dst_path']) == False:
                                log( "'{}' task failed. '{}' does not exist".format( row['title'], row['dst_path'] ), _ERROR )
                                failures += 1
                            if failures == 0:
                                cmd = row['command']
                                cmd=cmd.replace( "_ffmpeg", _FFMPEG )
                                cmd=cmd.replace( "_src", the_url )
                                dst_file_1 = "" # created from src file
                                dst_file_2 = "" # created from src file
                                dst_file_3 = "" # created from src file
                                if row['title'] == "jpeg+mpg+wav creation":
                                    src_file_list = src_file.split('.')
                                    dst_file_1 = "{}%02d.jpg".format( src_file_list[0] )
                                    dst_file_2 = "{}.mpg".format( src_file_list[0] )
                                    dst_file_3 = "{}.wav".format( src_file_list[0] )
                                
                                cmd=cmd.replace( "_dest1_", row['dst_path']+'/'+dst_file_1 )
                                cmd=cmd.replace( "_dest2_", row['dst_path']+'/'+dst_file_2 )
                                cmd=cmd.replace( "_dest3_", row['dst_path']+'/'+dst_file_3 )
                                creatives_list.append( "{}/arc_jpg/{}01.jpg".format( _CONVERTER_DEST_PATH, src_file_list[0] ) )
                                creatives_list.append( "{}/arc_mpg/{}".format( _CONVERTER_DEST_PATH, dst_file_2 ) )
                                creatives_list.append( "{}/arc_wav/{}".format( _CONVERTER_DEST_PATH, dst_file_3 ) )
                                log( "executing task '{}' cmd '{}'".format( row['title'], cmd ) )
                                res=os.system( cmd )
                                met = int(round(time.time() * 1000 ) ) - met
                                if res != 0:
                                    log( "'{}' task failed in {}. '{}' returned non-zero result [{}].".format( row['title'], str(met)+"ms", cmd, res ), _ERROR )
                                    failures += 1
                                else:
                                    log( "'{}' task succeeded in {}. '{}' returned result [{}].".format( row['title'], str(met)+"ms", cmd, res ), _INFO )

                            row = c.fetchone()
                        log( "worker[{}] - work complete".format( os.getpid() ) )
                        et = int(round(time.time() * 1000 ) ) - et


                        log( "copy, then delete files {} wav, mpg, jpgs".format( src_file_list[0] ) )
                        rsyncOK = 0
                        wav_file = "tmp/{}.wav".format( src_file_list[0] )
                        wav_path = "{}/arc_wav".format( _CONVERTER_DEST_PATH )
                        log( "copying {} to {}".format( wav_file, wav_path) )
                        cmd = "{} -vx {} {}".format( _RSYNC, wav_file, wav_path )
                        log( cmd )
                        res = os.system( cmd )
                        rsyncOK = rsyncOK + res
                        log( "rsync returned '{}' for an exit code".format( res ))

                        mpg_file = "tmp/{}.mpg".format( src_file_list[0] )
                        mpg_path = "{}/arc_mpg".format( _CONVERTER_DEST_PATH )
                        log( "copying {} to {}".format( mpg_file, mpg_path) )
                        cmd = "{} -vx {} {}".format( _RSYNC, mpg_file, mpg_path )
                        log( cmd )
                        res = os.system( cmd )
                        rsyncOK = rsyncOK + res
                        log( "rsync returned '{}' for an exit code".format( res ))

                        jpg_files = "tmp/{}*.jpg".format( src_file_list[0] )
                        jpg_path = "{}/arc_jpg".format( _CONVERTER_DEST_PATH )
                        log( "copying {} to {}".format( jpg_files, jpg_path) )
                        cmd = "{} -vx {} {}".format( _RSYNC, jpg_files, jpg_path )
                        log( cmd )
                        res = os.system( cmd )
                        rsyncOK = rsyncOK + res
                        log( "rsync returned '{}' for an exit code".format( res ))

                        #if rsyncOK == 0:
                        log( "removing wav file {}".format ( wav_file ) )
                        os.unlink( "{}".format( wav_file ) )
                        log( "removing mpg file {}".format ( mpg_file ) )
                        os.unlink( "{}".format( mpg_file ) )
                        log( "removing jpg files {}".format ( jpg_files ) )
                        cmd = "rm -f {}".format( jpg_files )
                        res = os.system( cmd )

                        # check that creatives landed safely
                        for creative in creatives_list:
                            if os.path.exists( creative ) is False:
                                log( "creative '{}' does not exist".format( creative), _ERROR )
                                failures += 1
                            else:
                                log( "creative '{}' does exist".format(creative), _INFO )

                        if failures > 0:
                            sql = "update jobs set status=%s, result=%s, elapsed_time=%s where id=%s"
                            log( sql, _INFO )
                            c.execute( sql, (_COMPLETE, "failed", str( et )+"ms", job_id)  )
                        else:
                            sql = "update jobs set status='{}', result='{}', elapsed_time='{}' where id='{}'".format( _COMPLETE, "success", str( et )+"ms", job_id )
                            log( sql, _INFO )
                            c.execute( sql )
                        
                        os._exit(1)

                    else:
                        processes[pid]['state'] = _PROCESSING
                        processes[pid]['start'] = int( time.time() )
                        processes[pid]['stop'] = 0
                        processes[pid]['id'] = id
                        num_workers += 1
                else:
                    log( "all workers busy..." )
            else:
                log( "schedule id '{}' - all jobs underway".format( sched_id ), _INFO )
                jobs_done = 1

            #log( "*** workers *** " )
            log( "workers @ work: [{}/{}]".format( num_workers, _MAX_WORKERS ) )
            jobs_still_running = 0
            for pid, info in processes.items():
                et=""
                if processes[pid]['state'] != _COMPLETE:
                    jobs_still_running += 1
                    alive=os.system( "kill -0 {} 2>/dev/null".format( pid ) )
                    stop=0
                    if alive==0:
                        res=os.waitpid( pid, os.WNOHANG)
                    else:
                        processes[pid]['stop'] = int( time.time() )
                        processes[pid]['state'] = _COMPLETE
                        #sql = "update jobs set status=%s where id=%s"
                        #log( sql, _DEBUG )
                        #c.execute( sql, (_COMPLETE, processes[pid]['id'])  )
                        num_workers -= 1
                        et = processes[pid]['stop'] - processes[pid]['start']
                        log( "worker[{}] state[{}] elapsed_time[{}ms] start[{}] stop[{}]". format( pid, _STATES[processes[pid]['state']], et, processes[pid]['start'], processes[pid]['stop'] ) )

            if jobs_still_running == 0: # we're done here
                log( "schedule id '{}' - all jobs completed".format( sched_id ), _INFO )
                done = 1
            time.sleep( 1 )
        #end while
        o_et = int(round(time.time() * 1000 ) ) - o_et
        sql = "update schedule set status='{}', elapsed_time='{}' where id='{}'".format( _COMPLETE, o_et, sched_id )
        log( sql, _DEBUG )
        c.execute( sql )                
    log( "heartbeat" )
    time.sleep( 1 )

# close cursors
c.close()
oc.close()
    
#close connections
conn.close()
oconn.close()
     



























    
