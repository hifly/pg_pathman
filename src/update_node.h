#ifndef UPDATE_NODE_H
#define UPDATE_NODE_H

#include "nodes/plannodes.h"
#include "nodes/execnodes.h"

typedef struct
{
	CustomScanState		css;
	Plan			   *subplan;
	// PartRelationInfo   *prel;
	PlanState		   *subplanstate;    /* original scan state */
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

#endif