#include "pathman.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "storage/dsm.h"
#include "access/xact.h"
#include "utils.h"
#include "utils/snapmgr.h"
#include "utils/typcache.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/fmgroids.h"
#include "funcapi.h"

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
 * Second one is to distribute data among partitions. It divides data into
 * batches and start new transaction for each batch.
 *-------------------------------------------------------------------------
 */

#define WORKER_SLOTS 10

static void start_bg_worker(bgworker_main_type main_func, int arg, bool wait);
static void create_partitions_bg_worker_main(Datum main_arg);
static void partition_data_bg_worker_main(Datum main_arg);

PG_FUNCTION_INFO_V1( partition_data_worker );
PG_FUNCTION_INFO_V1( active_workers );
PG_FUNCTION_INFO_V1( stop_worker );

typedef struct CreatePartitionsArgs
{
	Oid			dbid;
	Oid			relid;
	int64		value;
	Oid			value_type;
	bool		by_val;
	Oid			result;
	bool		crashed;
} CreatePartitionsArgs;

typedef enum WorkerStatus
{
	WS_FREE = 0,
	WS_WORKING,
	WS_STOPPING
} WorkerStatus;

typedef struct PartitionDataArgs
{
	WorkerStatus	status;
	RelationKey	key;
	uint32		batch_size;
	uint32		batch_count;
	pid_t		pid;
	size_t		total_rows;
} PartitionDataArgs;

PartitionDataArgs *slots;

/*
 * Initialize shared memory
 */
void
create_worker_slots()
{
	bool	found;
	size_t	size = get_worker_slots_size();

	slots = (PartitionDataArgs *)
		ShmemInitStruct("worker slots", size ,&found);

	if (!found)
		memset(slots, 0, size);
}

size_t
get_worker_slots_size(void)
{
	return sizeof(PartitionDataArgs) * WORKER_SLOTS;
}

/*
 * Common function to start background worker
 */
static void
start_bg_worker(bgworker_main_type main_func, int arg, bool wait)
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
	pid_t					pid;

	/* Initialize worker struct */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = main_func;
	worker.bgw_main_arg = Int32GetDatum(arg);
	worker.bgw_notify_pid = MyProcPid;

	/* Start dynamic worker */
	bgw_started = RegisterDynamicBackgroundWorker(&worker, &bgw_handle);
	HandleError(bgw_started == false, BGW_COULD_NOT_START);

	/* Wait till the worker starts */
	bgw_status = WaitForBackgroundWorkerStartup(bgw_handle, &pid);
	HandleError(bgw_status == BGWH_POSTMASTER_DIED, BGW_PM_DIED);

	elog(NOTICE, "worker pid: %u", pid);
	// sleep(30);

	if(wait)
	{
		/* Wait till the worker finishes job */
		bgw_status = WaitForBackgroundWorkerShutdown(bgw_handle);
		HandleError(bgw_status == BGWH_POSTMASTER_DIED, BGW_PM_DIED);
	}

/* end execution */
handle_exec_state:

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
	size_t					args_size = sizeof(CreatePartitionsArgs);
	TypeCacheEntry		   *tce;
	Oid						child_oid;
	dsm_segment			   *segment;
	dsm_handle				segment_handle;
	void				   *segment_pointer;

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

	/* Create a dsm segment for the worker to pass arguments */
	segment = dsm_create(args_size, 0);
	segment_handle = dsm_segment_handle(segment);

	/* Fill arguments structure */
	segment_pointer = dsm_segment_address(segment);
	memcpy(segment_pointer, args, args_size);

	/* start worker and wait for it to finish */
	start_bg_worker(create_partitions_bg_worker_main,
					segment_handle,
					true);

	/* Copy resulting data from dsm back into private memory */
	memcpy(args, segment_pointer, args_size);
	dsm_detach(segment);

	LWLockRelease(pmstate->load_config_lock);
	LWLockRelease(pmstate->edit_partitions_lock);

	child_oid = args->result;
	pfree(args);
	return child_oid;
}

/*
 * Starts background worker that redistributes data. Function returns
 * immediately
 */
void
partition_data_bg_worker(Oid relid)
{
	PartitionDataArgs  *args = NULL;
	int 	empty_slot_idx = -1;
	int 	i;

	/* TODO: lock would be nice */

	/* Check if relation is a partitioned table */
	if (get_pathman_relation_info(relid, NULL) == NULL)
		elog(ERROR,
			 "Relation '%s' isn't partitioned by pg_pathman",
			 get_rel_name(relid));

	/*
	 * Look for empty slot and also check that partitioning data for this table
	 * hasn't already starded
	 */
	for (i=0; i<WORKER_SLOTS; i++)
	{
		if (slots[i].status == WS_FREE)
		{
			if (empty_slot_idx < 0)
			{
				args = &slots[i];
				empty_slot_idx = i;
			}
		}
		else if (slots[i].key.relid == relid && slots[i].key.dbid == MyDatabaseId)
		{
			elog(ERROR,
				 "Table '%s' is already being partitioned",
				 get_rel_name(relid));
		}

	}

	if (args == NULL)
		elog(ERROR, "No empty worker slots found");

	/* Fill arguments structure */
	args->status = WS_WORKING;
	args->key.dbid = MyDatabaseId;
	args->key.relid = relid;
	args->total_rows = 0;

	/* start worker and wait for it to finish */
	start_bg_worker(partition_data_bg_worker_main,
					empty_slot_idx,
					false);
}

/*
 * PL wrapper for partition_data_bg_worker() func
 */
Datum
partition_data_worker( PG_FUNCTION_ARGS )
{
	(void) partition_data_bg_worker(PG_GETARG_OID(0));
	PG_RETURN_VOID();
}

/*
 * Main worker routine. Accepts dsm_handle as an argument
 */
static void
create_partitions_bg_worker_main(Datum main_arg)
{
	CreatePartitionsArgs *args;
	dsm_segment	   *segment;
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
	Oid					types[]	= { OIDOID,					 value_type };
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

		ret = SPI_execute_with_args(sql, 2, types, vals, nulls, false, 0);
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

/*
 * Main worker routine. Accepts dsm_handle as an argument
 */
static void
partition_data_bg_worker_main(Datum main_arg)
{
	PartitionDataArgs   *args;
	char		   *schema;
	char		   *sql = NULL;
	Oid				types[2]	= { OIDOID,	INT4OID };
	Datum			vals[2];
	bool			nulls[2]	= { false, false };
	int rows;
	int slot_idx = DatumGetInt32(main_arg);
	MemoryContext	worker_context = CurrentMemoryContext;

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "PartitionDataWorker");

	args = &slots[slot_idx];
	args->pid = MyProcPid;
	vals[0] = args->key.relid;
	vals[1] = 10000;

	sleep(20);

	/* Establish connection and start transaction */
	BackgroundWorkerInitializeConnectionByOid(args->key.dbid, InvalidOid);

	do
	{
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());

		if (sql == NULL)
		{
			MemoryContext oldcontext;

			schema = get_extension_schema();

			/*
			 * Allocate as SQL query in top memory context because current
			 * context will be destroyed after transaction finishes
			 */
			oldcontext = MemoryContextSwitchTo(worker_context);
			sql = psprintf("SELECT %s.batch_partition_data($1, p_limit:=$2)", schema);
			MemoryContextSwitchTo(oldcontext);
		}

		PG_TRY();
		{
			int		ret;
			bool	isnull;

			ret = SPI_execute_with_args(sql, 2, types, vals, nulls, false, 0);
			if (ret > 0)
			{
				TupleDesc	tupdesc = SPI_tuptable->tupdesc;
				HeapTuple	tuple = SPI_tuptable->vals[0];

				Assert(SPI_processed == 1);

				rows = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &isnull));
			}
		}
		PG_CATCH();
		{
			pfree(sql);
			pfree(schema);
			args->status = WS_FREE;
			elog(ERROR, "Partition data failed");
		}
		PG_END_TRY();

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();

		args->total_rows += rows;

		/* If other backend requested to stop worker then quit */
		if (args->status == WS_STOPPING)
			break;
	}
	while(rows > 0);  /* do while there is still rows to relocate */

	pfree(sql);
	pfree(schema);
	args->status = WS_FREE;
}

/* Function context for active_workers() SRF */
typedef struct PartitionDataListCtxt
{
	int			cur_idx;
} PartitionDataListCtxt;

/*
 * Returns list of active workers for partitioning data. Each record
 * contains pid, relation name and number of processed rows
 */
Datum
active_workers(PG_FUNCTION_ARGS)
{
	FuncCallContext    *funcctx;
	MemoryContext		oldcontext;
	TupleDesc			tupdesc;
	PartitionDataListCtxt *userctx;
	int					i;
	Datum				result;
	
	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		/* Switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		userctx = (PartitionDataListCtxt *) palloc(sizeof(PartitionDataListCtxt));
		userctx->cur_idx = 0;
		funcctx->user_fctx = (void *) userctx;

		/* Create tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(4, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pid",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "relation",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "processed",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "status",
						   TEXTOID, -1, 0);
		funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	userctx = (PartitionDataListCtxt *) funcctx->user_fctx;

	/*
	 * Iterate through worker slots
	 */
	for (i=userctx->cur_idx; i<WORKER_SLOTS; i++)
	{
		if (slots[i].status != WS_FREE)
		{
			char	   *values[4];
			char		txtpid[16];
			char		txtrows[16];
			HeapTuple	tuple;

			sprintf(txtpid, "%d", slots[i].pid);
			sprintf(txtrows, "%lu", slots[i].total_rows);
			values[0] = txtpid;
			values[1] = get_rel_name(slots[i].key.relid);
			values[2] = txtrows;
			switch(slots[i].status)
			{
				case WS_WORKING:
					values[3] = "working";
					break;
				case WS_STOPPING:
					values[3] = "stopping";
					break;
				default:
					values[3] = "unknown";
			}

			tuple = BuildTupleFromCStrings(funcctx->attinmeta, values);

			result = HeapTupleGetDatum(tuple);
			userctx->cur_idx = i + 1;
			SRF_RETURN_NEXT(funcctx, result);
		}
	}

	SRF_RETURN_DONE(funcctx);
}

Datum
stop_worker(PG_FUNCTION_ARGS)
{
	Oid		relid = PG_GETARG_OID(0);
	int		i;

	// PG_TRY();
	// {
	// 	relid = DatumGetObjectId(OidFunctionCall1(F_TO_REGCLASS, relname));
	// }
	// PG_CATCH();
	// {
	// 	elog(ERROR, "Couldn't find relation '%s'", DatumGetCString(relname));
	// 	return InvalidOid; /* compiler should be happy */
	// }
	// PG_END_TRY();

	for (i = 0; i < WORKER_SLOTS; i++)
		if (slots[i].key.relid == relid && slots[i].key.dbid == MyDatabaseId)
		{
			// TerminateBackgroundWorker(&slots[i].bgw_handle);
			slots[i].status = WS_STOPPING;
			PG_RETURN_BOOL(true);
		}

	elog(ERROR,
		 "Worker for relation '%s' not found",
		 get_rel_name(relid));
}
