#include "pathman.h"
#include "update_node.h"
#include "nodes/nodeFuncs.h"
#include "executor/executor.h"


CustomScanMethods	update_plan_methods;
CustomExecMethods	update_exec_methods;

void
setup_update_exec_methods()
{
	update_exec_methods.CustomName = "UpdateNode";
	update_exec_methods.BeginCustomScan = BeginUpdateScan;
	update_exec_methods.ExecCustomScan = ExecUpdateScan;
	update_exec_methods.EndCustomScan = EndUpdateScan;
	update_exec_methods.ReScanCustomScan = ReScanUpdateScan;
	update_exec_methods.MarkPosCustomScan = NULL;
	update_exec_methods.RestrPosCustomScan = NULL;
	
	update_plan_methods.CreateCustomScanState = CreateUpdateScanState;
	update_plan_methods.CustomName = "UpdateNode";
}

void
add_filter(Plan *plan)
{
	if (plan == NULL)
		return;

	switch (nodeTag(plan))
	{
		case T_ModifyTable:
			{
				ModifyTable *mt = (ModifyTable *) plan;
				ListCell *lc;

				foreach (lc, mt->plans)
				{
					lfirst(lc) = CreateUpdateScanPlan(lfirst(lc));
				}
			}
			break;
		default:
			return;
	}
}

/*
 * Build partition filter's target list pointing to subplan tuple's elements
 */
static List *
pfilter_build_tlist(List *tlist)
{
	List	   *result_tlist = NIL;
	ListCell   *lc;
	int			i = 1;

	foreach (lc, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		Var *var = makeVar(INDEX_VAR,	/* point to subplan's elements */
						   i,			/* direct attribute mapping */
						   exprType((Node *) tle->expr),
						   exprTypmod((Node *) tle->expr),
						   exprCollation((Node *) tle->expr),
						   0);

		result_tlist = lappend(result_tlist,
							   makeTargetEntry((Expr *) var,
											   i,
											   NULL,
											   tle->resjunk));
		i++; /* next resno */
	}

	return result_tlist;
}


// CustomPlan *
// CreateUpdateScanPlan(Plan *subplan, Oid partitioned_table,
// 					 OnConflictAction conflict_action)
Plan *
CreateUpdateScanPlan(Plan *subplan)
{
	CustomScan *cscan = makeNode(CustomScan);

	cscan->scan.plan.startup_cost = subplan->startup_cost;
	cscan->scan.plan.total_cost = subplan->total_cost;
	cscan->scan.plan.plan_rows = subplan->plan_rows;
	cscan->scan.plan.plan_width = subplan->plan_width;

	cscan->methods = &update_plan_methods;
	cscan->custom_plans = list_make1(subplan);

	// cscan->scan.plan.targetlist = pfilter_build_tlist(subplan->targetlist);
	cscan->scan.plan.targetlist = subplan->targetlist;

	/* No relation will be scanned */
	cscan->scan.scanrelid = 0;
	cscan->custom_scan_tlist = subplan->targetlist;

	/* Pack partitioned table's Oid and conflict_action */
	// cscan->custom_private = list_make2_int(partitioned_table,
	// 									   conflict_action);

	// return &cscan->scan.plan;
	return (Plan *) cscan;
}

Node *
CreateUpdateScanState(CustomScan *node)
{
	UpdateScanState   *state = palloc0(sizeof(UpdateScanState));

	NodeSetTag(state, T_CustomScanState);

	state->css.flags = node->flags;
	state->css.methods = &update_exec_methods;

	/* Extract necessary variables */
	state->subplan = (Plan *) linitial(node->custom_plans);
	// state->partitioned_table = linitial_int(node->custom_private);
	// state->onConflictAction = lsecond_int(node->custom_private);

	// /* Check boundaries */
	// Assert(state->onConflictAction >= ONCONFLICT_NONE ||
	// 	   state->onConflictAction <= ONCONFLICT_UPDATE);

	// /* Prepare dummy Const node */
	// NodeSetTag(&state->temp_const, T_Const);
	// state->temp_const.location = -1;
	state->subplanstate = NULL;

	return (Node *) state;
}

void
BeginUpdateScan(CustomScanState *node, EState *estate, int eflags)
{
	PlanState *substate;
	UpdateScanState *sstate = (UpdateScanState *) node;

	substate = ExecInitNode(sstate->subplan, estate, eflags);
	sstate->subplanstate = substate;
}

TupleTableSlot *
ExecUpdateScan(CustomScanState *node)
{
	PlanState *substate = ((UpdateScanState *) node)->subplanstate;

	return ExecProcNode(substate);
}

void
EndUpdateScan(CustomScanState *node)
{
	ExecEndNode(((UpdateScanState *) node)->subplanstate);
}

void
ReScanUpdateScan(CustomScanState *node)
{
	ExecProcNode(((UpdateScanState *) node)->subplanstate);
}
