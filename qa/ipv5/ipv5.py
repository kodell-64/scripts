# Korey O'Dell, 2019
#
#
import os.path
import subprocess
import json
import sys
import time
import select
import glob

FATAL=2**0
PASS_TEST=2**16
# IPv5 creates native TS files and audio only TS files.

module="IPv5"
binary="./ffmpeg_g"
test=""

def log( msg ):
    print("** {} #{} '{}'".format( module, test, msg ) )


    
def usage():
    print("****************************************************************")
    print("**                                                            **")
    print("** ********* Regression Test Module - ",format( sys.argv[0] ))
    print("**                                                            **")
    print("Usage: {}", sys.argv[0], " show_tests|run")
    print("**                                                            **")
    print("****************************************************************")


def show_tests():
    keys=sorted( tests.keys() )
    print("****************************************************************")
    print("**                                                            **")
    print("** ********* Regression Test Module - ",format( sys.argv[0] ))
    print("**                                                            **")
    print("** Available Tests - ")
    print("**                                                            **")
    for key in keys:
        {
        print("** {} => {}".format( key, tests[key]["desc"]) )
        }
    
capture_dir = "e/sd"
        
tests = {
    "100": {
        "desc": "Bail if logs directory does not exist.",
        "prepare_run": [
            "tcpplay -f test_files/fnc-1.dump -a 234.1.1.1 -p 5500",
            "rm -rf logs", 
        ]
        ,
        "length" : 10,
        "run": [
            "./ffmpeg_g -y -i udp://@127.1.1.1:5500 -vb 8000k -vf scale=720x400,pullup,fps=30.00 -acodec mp2 -ab 192k -ar 44100 -ac 2 -f mpegts e/sd/ZAC-orig.ts -c copy e/sd/ZAC.ts"
        ],
        "outputs": 
            {
                "isin_stderr":
                {
                    "PR-error: could not open log directory. Ensure that ./logs exists." : PASS_TEST
                }
            }

    },
    "200": {
        "desc": "Base case functionality of IPv5. Discard \"legacy\" SD files. Write native TS to 2m files.",
        "prepare_run": [
            "killall ffmpeg_g",
            "mkdir logs", "rm -rf {}".format( capture_dir ),
            "mkdir -p {}".format( capture_dir )
        ]
        ,
        "length" : 10,
        "inputs" : [
            "tcpplay -f test_files/fnc-1.dump -a 127.1.1.1 -p 5500"
        ],
        "run": [
            "./ffmpeg_g -y -i udp://@127.1.1.1:5500 -vb 8000k -vf scale=720x400,pullup,fps=30.00 -acodec mp2 -ab 192k -ar 44100 -ac 2 -f mpegts e/sd/ZAC-orig.ts -c copy e/sd/ZAC.ts",

        ],

        "outputs":
        {
            "post_run":
            {
                "check_files_exist":
                [
                    "e/sd/ZA*.EN2", "e/sd/ZA*.pic", "e/sd/ZA*.hsd", "e/sd/ZA*.sem", "e/sd/ZA*.sec",
                    "e/sd/ZA*.dat", "e/sd/ZA*.PAT", "e/sd/ZA*.bdx", "e/sd/ZA*.ts", 
                ]
            }
        },
    },

}

if( len(sys.argv) < 2 ):
    usage()
    exit(1)

if( sys.argv[1] == "show_tests" ):
    show_tests()
    exit(1)

tests_to_run = list();
if( sys.argv[1] == "run" and sys.argv[2] == "all" ):
    print( tests.keys() )
    tests_to_run = tests.keys()
elif( sys.argv[1] == "run" and sys.argv[2] ):
    tests_to_run.append( sys.argv[2] )

for test in tests_to_run:
    log( "\n******************************************************************" )
    log( "Running {}".format( tests[test]["desc"] ) )
    #log( "Cmd {} {}".format( binary, tests[test]["inputs"] ) )


    # prepare_run
    pre_run = list();
    for prep in tests[test]["prepare_run"]:
        log( "pre_run: starting {}".format( prep ) )
        pre_run.append ( subprocess.Popen( prep.split( ),
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True,
                                universal_newlines=True ) )
        time.sleep(1)
    # run
    running_process = ""
    for run in tests[test]["run"]:
        log( "run: starting {}".format( run ) )
        running_process = subprocess.Popen( run.split( ),
                                            stdout=subprocess.PIPE,
                                            stderr=open("stderr.log", "w"), close_fds=True,
                                            #stderr=subprocess.PIPE, close_fds=True,
                            universal_newlines=True ) 


    #process = subprocess.run( [binary] + tests[test]["inputs"].split(), shell=True, check=True,
    #                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT );
    #log( "pid {}".format( pid.pid ) )
    stderr_logfile = open('stderr.log', 'w')
    running = 2
    running_time = 0
    tests_passed = 0
    inputs_started = 0
    inputs = list()
    test_failures = 0;
    while( running ):

        # any inputs to start?
        proceed=0
        try:
            tests[test]["inputs"]
            proceed=1
        except KeyError:
            pass
        if( proceed and not inputs_started ):
            inputs_started = 1
            for input in tests[test]["inputs"]:
                log( "inputs: starting {}".format( input ) )
                inputs.append ( subprocess.Popen( input.split( ),
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True,
                                universal_newlines=True ) )


            
        # service/check STDERR
        stderr_result = 0
        #r, w, e = select.select([ running_process.stderr ], [], [], 0.0)
        #if running_process.stderr in r:
        #    stderr_logfile.write( running_process.stderr.read() )            
        #stdout, stderr = running_process.communicate()
        proceed=0
        try:
            tests[test]["outputs"]["isin_stderr"]
            proceed=1
        except KeyError:
            pass
        if( proceed ):    
            if( 1 ): #os.path.getsize( "stderr.log" ) > 0 ):


                log( "checking stderr" )
                stderr_read_logfile = open('stderr.log', 'r')
                conditions = tests[test]["outputs"]["isin_stderr"].keys()
                for key in conditions:

                    for line in stderr_read_logfile:
                        if key in line:
                            log( "checking for {} in stderr.log - [FOUND]".format( key ))
                            stderr_result += tests[test]["outputs"]["isin_stderr"][key]
                if( stderr_result == 0):
                    log( "checking for {} in stderr.log - [! FOUND]".format( key ))
                stderr_read_logfile.close();


            
        stderr_logfile.flush()
        #os.fsync()
        time.sleep(1)
        running_time += 1
        log( "running time of test [{}]".format( running_time ) )
        if( (stderr_result & PASS_TEST) ):
            tests_passed += 1
            log( "Test passed." )
        if( running_process.poll() is not None ):
            log( "process has exited." )
            running -= 1
        if( running_time > int( tests[test]["length"] ) ):
            log( "test length reached..." )
            running -= 1

    # POST RUN CHECKS
    proceed=0
    try:
        tests[test]["outputs"]["post_run"]["check_files_exist"]
        proceed=1
    except KeyError:
        pass
    if( proceed ):    
        check_files = tests[test]["outputs"]["post_run"]["check_files_exist"]
        for check_file in check_files:
            files = glob.glob( check_file )
            if files:
                log( "post_run: check_file {} [{}] exists, PASSED".format( check_file, files[0] ) )
            else:
                log( "post_run: check_file {} exists, FAILED".format( check_file ) )
                test_failures += 1
    # shutdown pre-run processes, if any
    for process in pre_run:
        log( "stopping process pre-run [{}]".format( process.pid ))
        process.terminate()
    for process in inputs:
        log( "stopping input process [{}]".format( process.pid ))
        process.terminate()
    running_process.terminate()

    log( "Test failures [{}]".format( test_failures ) )
    exit( test_failures )
