#ifndef UPDATE_NODE_H
#define UPDATE_NODE_H

#include "nodes/plannodes.h"
#include "nodes/execnodes.h"

// struct ProxyScanState;

// typedef struct
// {
// 	CustomScan			cs;
// 	List			   *subplans;
// 	ModifyTable		   *update;
// };

/*
 * Proxy scan state
 */
typedef struct
{
	CustomScanState		css;
	CustomScan		   *parent;
	// TupleTableSlot	   *tupleslot;
} ProxyScanState;

typedef struct
{
	CustomScanState		css;
	Plan			   *subplan;
	Plan			   *proxy;
	ModifyTable		   *insertplan;
	// PartRelationInfo   *prel;
	PlanState		   *subplanstate;	/* original scan state */
	PlanState		   *updatestate;	/* original update scan state */
	ProxyScanState	   *proxystate;
} UpdateScanState;

extern CustomScanMethods	update_plan_methods;
extern CustomExecMethods	update_exec_methods;

Plan *CreateUpdateScanPlan(Plan *subplan);
Node *CreateUpdateScanState(struct CustomScan *cscan);

void BeginUpdateScan(CustomScanState *node, EState *estate, int eflags);
TupleTableSlot *ExecUpdateScan(CustomScanState *node);
void EndUpdateScan(CustomScanState *node);
void ReScanUpdateScan(CustomScanState *node);

void setup_update_exec_methods(void);
void add_filter(Plan *plan);



extern CustomScanMethods	proxy_plan_methods;
extern CustomExecMethods	proxy_exec_methods;

CustomScan *CreateProxyPlan(Plan *subplan);
Node *CreateProxyScanState(CustomScan *node);
void BeginProxyScan(CustomScanState *node, EState *estate, int eflags);
TupleTableSlot *ExecProxyScan(CustomScanState *node);
void EndProxyScan(CustomScanState *node);
void ReScanProxyScan(CustomScanState *node);

#endif