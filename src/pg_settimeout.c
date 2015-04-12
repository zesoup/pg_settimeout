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

#include <unistd.h>

#include "storage/fd.h"

#include <time.h>
#include <stdio.h>

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
    int taken;
    } Task;

static Task *_task;
static char *_query;
static char *_user;
static char *_database;

PG_FUNCTION_INFO_V1 (pg_settimeout);


void _PG_init(void);
void pg_settimeout_main( Datum params );
int bgw_attached_dsm( int* semaphore);


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
    CurrentResourceOwner = ResourceOwnerCreate(NULL, NAME);
    seg = dsm_attach( segment );
    if (seg == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("Could not attach to DSM")));

    _task = (Task*) palloc ( sizeof(Task) );
    ((Task*) dsm_segment_address(seg))->taken = 1;
    memcpy(_task, dsm_segment_address(seg), sizeof(Task));

    _query =     (char*) palloc(sizeof(char)*_task->querysize);
    _user  =     (char*) palloc(sizeof(char)*_task->usernamesize);
    _database  = (char*) palloc(sizeof(char)*_task->dbnamesize);

    memcpy(_query, (char*) dsm_segment_address(seg)+sizeof(Task),_task->querysize);
    memcpy(_user,  (char*) dsm_segment_address(seg)+sizeof(Task)+ _task->querysize,_task->usernamesize);
    memcpy(_database,  (char*) dsm_segment_address(seg)+sizeof(Task)+ _task->querysize + _task->usernamesize,_task->dbnamesize);

    elog(LOG,"pg_settimeout launched for  %s@%s in %dms\n",
         _user, _database, _task->timeout);
    }

void pg_settimeout_main( Datum params ) {

    static Latch signalLatch;
    int rc;
    uint32 segment = DatumGetUInt32( params );
    StringInfoData buf;

    initStringInfo(&buf);
    appendStringInfo(&buf, "-");

    SetCurrentStatementStartTimestamp();

    pgstat_report_activity(STATE_RUNNING, buf.data);

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


    ResetLatch(&signalLatch);

    rc = WaitLatch(&signalLatch,
                   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                   _task->timeout*1);
    ResetLatch(&signalLatch);
    BackgroundWorkerInitializeConnection(_database, _user );

    if (rc & WL_POSTMASTER_DEATH)
        proc_exit(1);

    /*
     * In case of a SIGHUP, just reload the configuration.
     */
    if (got_sighup) {
        got_sighup = false;
        ProcessConfigFile (PGC_SIGHUP);
        }

    StartTransactionCommand();
    SetCurrentStatementStartTimestamp();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());
    SPI_execute(_query, false, 5);
    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();

    proc_exit(0);
    }

void _PG_init(void) {
    BackgroundWorker worker;
    if (!process_shared_preload_libraries_in_progress)
        return;

    strcpy(worker.bgw_name, NAME);
    /* set up common data for all our workers */
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS
                       | BGWORKER_BACKEND_DATABASE_CONNECTION;

    /*worker.bgw_start_time = BgWorkerStart_RecoveryFinished;*/
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_main = pg_settimeout_main;

    worker.bgw_notify_pid = 0;

    /*
     * Now fill in worker-specific data, and do the actual registrations.
     */
    //RegisterBackgroundWorker(&worker);
    }

/*
 * See if the BGW manages to attach. Should be a matter of miliseconds.
 * Return Error if he didnt make it in time.
 */
int bgw_attached_dsm( int* semaphore) {
    static Latch signalLatch;
    int sleeptime = 10;
    int roundsleft = MAX_STARTUP_ROUNDS;

    while ( semaphore ==0) {
        if (roundsleft <= 0) {
            break;
            }
        ResetLatch(&signalLatch);
        WaitLatch(&signalLatch,
                  WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                  sleeptime);
        roundsleft = roundsleft -1;
        }
    return roundsleft;
    }


/*
 * Dynamically launch an SPI worker.
 */
Datum pg_settimeout(PG_FUNCTION_ARGS) {

    BackgroundWorker worker;
    BackgroundWorkerHandle *handle;

    BgwHandleStatus status;
    dsm_segment* segment;
    Task task;
    ResourceOwner oldowner;
    pid_t pid;

    char* targetuser;
    char* targetdb;
    char* targetquery;

    text       *query = PG_GETARG_TEXT_P(0);
    int 		timeout= PG_GETARG_INT32(1);


    worker.bgw_flags = BGWORKER_SHMEM_ACCESS
                       | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    worker.bgw_restart_time = -1; /* Never */
    worker.bgw_main = NULL; /* new worker might not have library loaded */

    sprintf(worker.bgw_library_name, "pg_settimeout");
    sprintf(worker.bgw_function_name, "pg_settimeout_main");
    snprintf(worker.bgw_name, BGW_MAXLEN, NAME);



    targetuser = GetUserNameFromId( GetUserId()  );
    targetdb   = DatumGetCString(current_database(NULL)) ;
    targetquery= text_to_cstring(query);

    /* Set up a dynamic shared memory for the Backgroundworker.
     * It contains information about the required job.
     *
     * To make sure postgres will allow the backgroundworker to attach to the dsm,
     * set the current resource owner to a name we'll use later again.
     * Once done, reset the resource owner.
     */


    oldowner = CurrentResourceOwner;
    CurrentResourceOwner = ResourceOwnerCreate(NULL, NAME);

    /* (+1 for \0 ) */
    task.querysize      = strlen(targetquery)+1;
    task.usernamesize   = strlen(targetuser)+1;
    task.dbnamesize     = strlen(targetdb)+1;

    task.timeout = timeout;
    task.taken = 0;

    segment = dsm_create(sizeof(Task)+task.querysize +task.usernamesize +task.dbnamesize  );

    memcpy( dsm_segment_address(segment), &task,  sizeof(Task));
    sprintf( ( (char*)dsm_segment_address(segment) + sizeof(Task) ), "%s",  targetquery );
    sprintf( ( (char*)dsm_segment_address(segment) + sizeof(Task) + task.querysize), "%s",  targetuser );
    sprintf( ( (char*)dsm_segment_address(segment) + sizeof(Task) + task.querysize + task.usernamesize), "%s",  targetdb );

    CurrentResourceOwner = oldowner;

    worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(segment));
    /* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
    worker.bgw_notify_pid = MyProcPid;

    if (!RegisterDynamicBackgroundWorker(&worker, &handle))
        PG_RETURN_NULL();

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

    if (bgw_attached_dsm( &((Task*) dsm_segment_address(segment))->taken )) {
        PG_RETURN_INT32(pid);
        }
    else {
        PG_RETURN_INT32(-1);
        }

    }
