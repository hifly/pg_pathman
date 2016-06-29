#include "pathman.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "storage/dsm.h"
#include "access/xact.h"
#include "utils/snapmgr.h"
#include "utils/typcache.h"
#include "utils.h"

/*-------------------------------------------------------------------------
 *
 * worker.c
 *
 * There are two background workers in this module
 * First one is to create partitions in a separate transaction. To do so we
 * create a separate background worker, pass arguments to it
 * (see CreatePartitionsArgs) and gather the result (which is the new partition
 * oid).
 *
 * Second one is to partition data. It divides data into batches and start new
 * transaction for each batch.
 *-------------------------------------------------------------------------
 */

static dsm_segment *segment;

static void start_bg_worker(bgworker_main_type main_func, void *args, size_t args_size, bool wait);
static void create_partitions_bg_worker_main(Datum main_arg);

typedef struct CreatePartitionsArgs
{
	Oid		dbid;
	Oid		relid;
	int64	value;
	Oid		value_type;
	bool	by_val;
	Oid		result;
	bool	crashed;
} CreatePartitionsArgs;

/*
 * Common function to start background worker
 */
static void
start_bg_worker(bgworker_main_type main_func, void *args, size_t args_size, bool wait)
{
#define HandleError(condition, new_state) \
	if (condition) { exec_state = (new_state); goto handle_exec_state; }

	/* Execution state to be checked */
	enum
	{
		BGW_OK = 0,				/* everything is fine (default) */
		BGW_COULD_NOT_START,	/* could not start worker */
		BGW_PM_DIED,			/* postmaster died */
		BGW_CRASHED				/* worker crashed */
	}						exec_state = BGW_OK;

	BackgroundWorker		worker;
	BackgroundWorkerHandle *bgw_handle;
	BgwHandleStatus			bgw_status;
	bool					bgw_started;
	dsm_segment			   *segment;
	dsm_handle				segment_handle;
	pid_t					pid;
	void				   *segment_pointer;

	/* Create a dsm segment for the worker to pass arguments */
	segment = dsm_create(args_size, 0);
	segment_handle = dsm_segment_handle(segment);

	/* Fill arguments structure */
	segment_pointer = dsm_segment_address(segment);
	memcpy(segment_pointer, args, args_size);

	/* Initialize worker struct */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = main_func;
	worker.bgw_main_arg = Int32GetDatum(segment_handle);
	worker.bgw_notify_pid = MyProcPid;

	/* Start dynamic worker */
	bgw_started = RegisterDynamicBackgroundWorker(&worker, &bgw_handle);
	HandleError(bgw_started == false, BGW_COULD_NOT_START);

	/* Wait till the worker starts */
	bgw_status = WaitForBackgroundWorkerStartup(bgw_handle, &pid);
	HandleError(bgw_status == BGWH_POSTMASTER_DIED, BGW_PM_DIED);

	if(wait)
	{
		/* Wait till the worker finishes job */
		bgw_status = WaitForBackgroundWorkerShutdown(bgw_handle);
		HandleError(bgw_status == BGWH_POSTMASTER_DIED, BGW_PM_DIED);

		/* Copy resulting data from dsm back into private memory */
		memcpy(args, segment_pointer, args_size);
	}

/* end execution */
handle_exec_state:
	dsm_detach(segment);

	switch (exec_state)
	{
		case BGW_COULD_NOT_START:
			elog(ERROR, "Unable to create background worker for pg_pathman");
			break;

		case BGW_PM_DIED:
			ereport(ERROR,
					(errmsg("Postmaster died during the pg_pathman background worker process"),
					errhint("More details may be available in the server log.")));
			break;

		case BGW_CRASHED:
			elog(ERROR, "Could not create partition due to background worker crash");
			break;

		default:
			break;
	}
}

/*
 * Starts background worker that will create new partitions,
 * waits till it finishes the job and returns the result (new partition oid)
 */
Oid
create_partitions_bg_worker(Oid relid, Datum value, Oid value_type)
{
	CreatePartitionsArgs   *args;
	TypeCacheEntry		   *tce;
	Oid						child_oid;

	tce = lookup_type_cache(value_type, 0);

	/* Fill arguments structure */
	args = palloc(sizeof(CreatePartitionsArgs));
	args->dbid = MyDatabaseId;
	args->relid = relid;
	if (tce->typbyval)
		args->value = value;
	else
		memcpy(&args->value, DatumGetPointer(value), sizeof(args->value));
	args->by_val = tce->typbyval;
	args->value_type = value_type;
	args->result = 0;

	LWLockAcquire(pmstate->load_config_lock, LW_EXCLUSIVE);
	LWLockAcquire(pmstate->edit_partitions_lock, LW_EXCLUSIVE);

	/* start worker and wait for it to finish */
	start_bg_worker(create_partitions_bg_worker_main,
					(void *)args,
					sizeof(CreatePartitionsArgs),
					true);

	LWLockRelease(pmstate->load_config_lock);
	LWLockRelease(pmstate->edit_partitions_lock);

	child_oid = args->result;
	pfree(args);
	return child_oid;
}

/*
 * Main worker routine. Accepts dsm_handle as an argument
 */
static void
create_partitions_bg_worker_main(Datum main_arg)
{
	CreatePartitionsArgs *args;
	dsm_handle		handle = DatumGetInt32(main_arg);

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "CreatePartitionsWorker");

	/* Attach to dynamic shared memory */
	if (!handle)
		ereport(WARNING, (errmsg("pg_pathman worker: invalid dsm_handle")));

	segment = dsm_attach(handle);
	args = dsm_segment_address(segment);

	/* Establish connection and start transaction */
	BackgroundWorkerInitializeConnectionByOid(args->dbid, InvalidOid);
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Create partitions */
	args->result = create_partitions(args->relid,
									 PATHMAN_GET_DATUM(args->value,
													   args->by_val),
									 args->value_type,
									 &args->crashed);

	/* Cleanup */
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	dsm_detach(segment);
}

/*
 * Create partitions and return an OID of the partition that contain value
 */
Oid
create_partitions(Oid relid, Datum value, Oid value_type, bool *crashed)
{
	Oid					oids[]	= { OIDOID,					 value_type };
	Datum				vals[]	= { ObjectIdGetDatum(relid), value };
	bool				nulls[]	= { false,					 false };
	char			   *sql;
	PartRelationInfo   *prel;
	RangeRelation	   *rangerel;
	FmgrInfo			cmp_func;
	char			   *schema;

	*crashed = true;
	schema = get_extension_schema();

	prel = get_pathman_relation_info(relid, NULL);
	rangerel = get_pathman_range_relation(relid, NULL);

	/* Comparison function */
	fill_type_cmp_fmgr_info(&cmp_func, value_type, prel->atttype);

	/* Perform PL procedure */
	sql = psprintf("SELECT %s.append_partitions_on_demand_internal($1, $2)",
				   schema);
	PG_TRY();
	{
		int				ret;
		Oid				partid = InvalidOid;
		bool			isnull;

		ret = SPI_execute_with_args(sql, 2, oids, vals, nulls, false, 0);
		if (ret > 0)
		{
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;
			HeapTuple	tuple = SPI_tuptable->vals[0];

			Assert(SPI_processed == 1);

			partid = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 1, &isnull));

			/* Update relation info */
			free_dsm_array(&rangerel->ranges);
			free_dsm_array(&prel->children);
			load_check_constraints(relid, GetCatalogSnapshot(relid));
		}

		*crashed = false;
		return partid;
	}
	PG_CATCH();
	{
		elog(ERROR, "Attempt to create new partitions failed");

		return InvalidOid; /* compiler should be happy */
	}
	PG_END_TRY();
}
