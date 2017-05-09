#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/shm_mq.h"

#include "commands/async.h"

/* these headers are used by this particular worker's code */
#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "storage/dsm.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "tcop/utility.h"
#include "assert.h" /*ASSERT*/

#include "utils/memutils.h"
#include "storage/shm_toc.h"
#include "storage/fd.h"
#include "sys/time.h"


#define NAME "pg_settimeout"
#define MAX_STARTUP_ROUNDS 10

PG_MODULE_MAGIC;

extern int errno;

typedef struct Task {
    int querysize;
    int usernamesize;
    int dbnamesize;

    int userID;
    int timeout;
    int repeat;
    int taken;
    } Task;


static Task *_task;
static char *_query;
static char *_user;
static char *_database;

PG_FUNCTION_INFO_V1 (pg_settimeout);
PG_FUNCTION_INFO_V1 (pg_setinterval);


void _PG_init(void);
void pg_settimeout_main( Datum params );
int pg_settimeout_core(text* query, int timeout, int repeat);
dsm_segment* provide_and_fill_dsm( text* query, int timeout, int repeat ) ;
int bgw_attached_dsm( int* semaphore);
void executeQuery(char* query);


/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void worker_spi_sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    printf("SIGTERM\n");
    got_sigterm = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);

    errno = save_errno;
    proc_exit(0);
    }

/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void worker_spi_sighup(SIGNAL_ARGS) {
    int save_errno = errno;
    printf("SIGHUB\n");
    got_sighup = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);

    errno = save_errno;
    }


/* attach worker to the shared memory segment, read the job structure */
static void
getTask(uint32 segment) {
    dsm_segment *seg=NULL;
    ResourceOwner oldowner;

    oldowner = CurrentResourceOwner;
    CurrentResourceOwner = ResourceOwnerCreate(NULL, NAME);

    seg = dsm_attach( segment );
    if (seg == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("Could not attach to DSM")));

    CurrentResourceOwner = oldowner;


    _task = (Task*) palloc ( sizeof(Task) );
    ((Task*) dsm_segment_address(seg))->taken = 1;
    memcpy(_task, dsm_segment_address(seg), sizeof(Task));

    _query =     (char*) palloc(sizeof(char)*_task->querysize);
    _user  =     (char*) palloc(sizeof(char)*_task->usernamesize);
    _database  = (char*) palloc(sizeof(char)*_task->dbnamesize);

    memcpy(_query, (char*) dsm_segment_address(seg)+sizeof(Task),_task->querysize);
    memcpy(_user,  (char*) dsm_segment_address(seg)+sizeof(Task)+ _task->querysize,_task->usernamesize);
    memcpy(_database,  (char*) dsm_segment_address(seg)+sizeof(Task)+ _task->querysize + _task->usernamesize,_task->dbnamesize);

    elog(LOG,"pg_settimeout launched for  %s@%s. Idle for %dms",
         _user, _database, _task->timeout);
    }

void executeQuery(char* query) {
    StartTransactionCommand();
    SetCurrentStatementStartTimestamp();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());
    SPI_exec(query, 0);
    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
    ProcessCompletedNotifies();
    }

void pg_settimeout_main( Datum params ) {

    static Latch signalLatch;
    int rc;

    uint32 segment = DatumGetUInt32( params );
    StringInfoData buf;

    initStringInfo(&buf);
    SetCurrentStatementStartTimestamp();

    /* The latch used for this worker to manage sleep correctly */

    if (MyProc == NULL) {
        InitializeLatchSupport();
        InitLatch(&signalLatch);
        }
    else {
        signalLatch = MyProc->procLatch;
        }

    pqsignal(SIGHUP, worker_spi_sighup);
    pqsignal(SIGTERM, worker_spi_sigterm);


    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

    getTask (segment);
    appendStringInfo(&buf, "%s",_query);

    /* Connect and Report early on, so we're listed in pg_stat_activity while waiting */
    BackgroundWorkerInitializeConnection(_database, _user );
    pgstat_report_appname("Backgroundworker - pg_settimeout");
    ResetLatch(&signalLatch);
    do {
        pgstat_report_activity(STATE_IDLE, buf.data);

        elog(LOG,"Worker hibernates for %dms",_task->timeout);
        rc = WaitLatch(&signalLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       _task->timeout*1, NULL);
        ResetLatch(&signalLatch);
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);
        pgstat_report_activity(STATE_RUNNING, buf.data);

        /* For now we dont care about the outcome. It may fail, but there's not alot the
         * worker can do about it. SPI-level Errors are handled by SPI.
         */
        elog(LOG,"Worker woke up.\"%s\" is due.", _query);
        executeQuery(_query);

        }
    while (_task->repeat);
    proc_exit(0);
    }

void _PG_init(void) {
    /* Nothing has to be done here as no gucs or startuproutines are in place.
     */
    }

/*
 * See if the BGW manages to attach. Should be a matter of miliseconds.
 * Return Error if he didnt make it in time.
 */
int bgw_attached_dsm( int* semaphore) {
    static Latch signalLatch;
    int sleeptime = 100;
    int roundsleft = MAX_STARTUP_ROUNDS;
    InitLatch( &signalLatch);

    while ( *semaphore ==0) {
        if (roundsleft <= 0) {
            break;
            }
        ResetLatch(&signalLatch);
        WaitLatch(&signalLatch,
                  WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                  sleeptime,NULL);
        roundsleft = roundsleft -1;
        }
    return roundsleft;
    }

/*
 * SQL interface for settimeout.
 */
Datum pg_settimeout(PG_FUNCTION_ARGS) {
    text       *query = PG_GETARG_TEXT_P(0);
    int               timeout= PG_GETARG_INT32(1);

    PG_RETURN_INT32( pg_settimeout_core(query, timeout, 0) );
    }
/*
 * SQL interface for setinterval
 */
Datum pg_setinterval(PG_FUNCTION_ARGS) {
    text       *query = PG_GETARG_TEXT_P(0);
    int         timeout= PG_GETARG_INT32(1);

    PG_RETURN_INT32( pg_settimeout_core(query, timeout, 1 ) );
    }


    /* Set up a dynamic shared memory for the Backgroundworker.
        * It contains information about the required job.
        *
        * To make sure postgres will allow the backgroundworker to attach to the dsm,
        * set the current resource owner to a name we'll use later again.
        * Once done, reset the resource owner.
        */
dsm_segment* provide_and_fill_dsm( text* query, int timeout, int repeat ) {
    ResourceOwner oldowner;
    dsm_segment* segment;

    Task task;
    char* targetuser;
    char* targetdb;
    char* targetquery;

    targetuser = GetUserNameFromId( GetUserId() , false );
    targetdb   = DatumGetCString(current_database(NULL)) ;
    targetquery= text_to_cstring(query);


    oldowner = CurrentResourceOwner;
    CurrentResourceOwner = ResourceOwnerCreate(NULL, NAME);

    /* +1 for null termination. Not necessary as the length is stored aswell.
     *  */
    task.querysize      = strlen(targetquery)+1;
    task.usernamesize   = strlen(targetuser)+1;
    task.dbnamesize     = strlen(targetdb)+1;

    task.timeout = timeout;
    task.taken = 0;
    task.repeat = repeat;

    segment = dsm_create(sizeof(Task)+task.querysize +task.usernamesize +task.dbnamesize,0  );

    /* Store the Task and append some strings */
    memcpy( dsm_segment_address(segment), &task,  sizeof(Task));
    sprintf(
        (char*)dsm_segment_address(segment) + sizeof(Task)
        , "%s",  targetquery );
    sprintf(
        (char*)dsm_segment_address(segment) + sizeof(Task) + task.querysize
        , "%s",  targetuser  );
    sprintf(
        (char*)dsm_segment_address(segment) + sizeof(Task) + task.querysize + task.usernamesize
        , "%s",  targetdb );

    CurrentResourceOwner = oldowner;
    return segment;
    }


int pg_settimeout_core(text* query, int timeout, int repeat) {

    BackgroundWorker worker;
    BackgroundWorkerHandle *handle;
    BgwHandleStatus status;
    pid_t pid;

    dsm_segment* segment;


    worker.bgw_flags = BGWORKER_SHMEM_ACCESS
                       | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    worker.bgw_restart_time = -1; /* Never */
    //worker.bgw_main = NULL; /* new worker might not have library loaded */

    sprintf(worker.bgw_library_name, "pg_settimeout");
    sprintf(worker.bgw_function_name, "pg_settimeout_main");
    snprintf(worker.bgw_name, BGW_MAXLEN, NAME);


    segment = provide_and_fill_dsm(query, timeout, repeat);


    worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(segment));
    /* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
    worker.bgw_notify_pid = MyProcPid;

    if (!RegisterDynamicBackgroundWorker(&worker, &handle))
        return -1;

    status = WaitForBackgroundWorkerStartup(handle, &pid);
    if (status == BGWH_STOPPED)
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg(
                     "could not start background process"), errhint(
                     "More details may be available in the server log.")));
    if (status == BGWH_POSTMASTER_DIED)
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg(
                     "cannot start background processes without postmaster"), errhint(
                     "Kill all remaining database processes and restart the database.")));

    /* At this point we should detach the dsm, but to prevent postgres from cleaning it up, we wont*/
    Assert(status == BGWH_STARTED);

    /* Postgres will remove the dsm if nobody uses it. So we'll need to wait untill the
     * BGW attaches to the memory and flips the flag.
     * TODO: Replace with postgres semaphores
     */
    if (!bgw_attached_dsm( &((Task*) dsm_segment_address(segment))->taken )) {
        elog(LOG,"pg_settimeout backgroundworker didnt access the dsm in time");
        pid = -1;
        }
    dsm_detach( segment );
    return pid;
    }
