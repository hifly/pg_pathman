#include "pathman.h"
#include "update_node.h"
#include "nodes/nodeFuncs.h"
#include "executor/executor.h"


CustomScanMethods	update_plan_methods;
CustomExecMethods	update_exec_methods;
CustomScanMethods	proxy_plan_methods;
CustomExecMethods	proxy_exec_methods;


static void ProxyPushTuple(CustomScan *proxy, TupleTableSlot *tuple);
static TupleTableSlot *ProxyPullTuple(CustomScan *proxy);
static void SetupProxyScanMethods(void);

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

	SetupProxyScanMethods();
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

// CreateUpdateScanPlan(Plan *subplan)
Plan *
CreateUpdateScanPlan(Plan *plan)
{
	CustomScan *cscan = makeNode(CustomScan);
	Plan *subplan;
	ModifyTable *mt;

	if (!IsA(plan, ModifyTable))
		return plan;

	mt = (ModifyTable *) plan;
	subplan = linitial(mt->plans);

	cscan->scan.plan.startup_cost = subplan->startup_cost;
	cscan->scan.plan.total_cost = subplan->total_cost;
	cscan->scan.plan.plan_rows = subplan->plan_rows;
	cscan->scan.plan.plan_width = subplan->plan_width;
	cscan->scan.plan.targetlist = subplan->targetlist;

	cscan->methods = &update_plan_methods;
	cscan->custom_private = mt->plans;
	// cscan->custom_plans = list_make1(subplan);

	// cscan->custom_private = list_make1(CreateProxyPlan(subplan));
	cscan->custom_private = list_make1(mt);

	/* No relation will be scanned */
	cscan->scan.scanrelid = 0;
	cscan->custom_scan_tlist = subplan->targetlist;

	// return &cscan->scan.plan;
	return (Plan *) cscan;
}

Node *
CreateUpdateScanState(CustomScan *node)
{
	UpdateScanState   *state = palloc0(sizeof(UpdateScanState));
	CustomScan *proxy;
	ModifyTable		  *mt;

	NodeSetTag(state, T_CustomScanState);

	state->css.flags = node->flags;
	state->css.methods = &update_exec_methods;

	/* Extract necessary variables */
	mt = (ModifyTable *) linitial(node->custom_private);
	/* TODO */
	state->subplan = (Plan *) linitial(mt->plans);
	state->insertplan = mt;

	//!
	proxy = CreateProxyPlan(state->subplan);
	mt->plans = list_make1(proxy);
	state->proxy = (Plan *) proxy;
	//!

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
	UpdateScanState *sstate = (UpdateScanState *) node;

	sstate->subplanstate = ExecInitNode(sstate->subplan, estate, eflags);
	sstate->proxystate = ExecInitNode(sstate->proxy, estate, eflags);
	sstate->updatestate = ExecInitNode((Plan *) sstate->insertplan, estate, eflags);
}

TupleTableSlot *
ExecUpdateScan(CustomScanState *node)
{
	UpdateScanState *state = (UpdateScanState *) node;
	TupleTableSlot *slot = ExecProcNode(state->subplanstate);

	ProxyPushTuple((CustomScan *) state->proxy, slot);
	slot = ExecProcNode(state->updatestate);
	return slot;
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


void
SetupProxyScanMethods()
{
	proxy_exec_methods.CustomName = "ProxyNode";
	proxy_exec_methods.BeginCustomScan = BeginProxyScan;
	proxy_exec_methods.ExecCustomScan = ExecProxyScan;
	proxy_exec_methods.EndCustomScan = EndProxyScan;
	proxy_exec_methods.ReScanCustomScan = ReScanProxyScan;
	proxy_exec_methods.MarkPosCustomScan = NULL;
	proxy_exec_methods.RestrPosCustomScan = NULL;
	
	proxy_plan_methods.CreateCustomScanState = CreateProxyScanState;
	proxy_plan_methods.CustomName = "ProxyNode";
}

CustomScan *
CreateProxyPlan(Plan *subplan)
{
	CustomScan *cscan = makeNode(CustomScan);

	cscan->scan.plan.startup_cost = subplan->startup_cost;
	cscan->scan.plan.total_cost = subplan->total_cost;
	cscan->scan.plan.plan_rows = subplan->plan_rows;
	cscan->scan.plan.plan_width = subplan->plan_width;
	cscan->scan.plan.targetlist = subplan->targetlist;

	cscan->methods = &proxy_plan_methods;
	cscan->custom_plans = list_make1(subplan);
	// cscan->custom_private = list_make1(NULL);

	/* No relation will be scanned */
	cscan->scan.scanrelid = 0;
	cscan->custom_scan_tlist = subplan->targetlist;

	return cscan;
}

void
ProxyPushTuple(CustomScan *proxy, TupleTableSlot *tuple)
{
	proxy->custom_private = lappend(proxy->custom_private, tuple);
}

TupleTableSlot *
ProxyPullTuple(CustomScan *proxy)
{
	TupleTableSlot *result = NULL;

	if (!proxy->custom_private)
		return NULL;

	result = linitial(proxy->custom_private);
	proxy->custom_private = list_delete_ptr(proxy->custom_private, result);
	return result;
}

Node *
CreateProxyScanState(CustomScan *node)
{
	ProxyScanState *state = (ProxyScanState *) palloc0(sizeof(ProxyScanState));

	NodeSetTag(state, T_CustomScanState);
	// state->tupleslot = NULL;
	state->css.methods = &proxy_exec_methods;
	state->parent = node;
	return (Node *) state;
}

void
BeginProxyScan(CustomScanState *node, EState *estate, int eflags)
{}

/* Just returns stored tuple slot */
TupleTableSlot *
ExecProxyScan(CustomScanState *node)
{
	ProxyScanState *state = (ProxyScanState *) node;
	CustomScan *plan = state->parent;

	return ProxyPullTuple(plan);
}

void EndProxyScan(CustomScanState *node)
{}

void ReScanProxyScan(CustomScanState *node)
{}