import Decimal from "decimal.js";
import { type TreeNode } from "@/src/features/traces/types/treeNode";

/** A sibling at or above this share of its parent is the parent's majority. */
const METRIC_EMPHASIS_THRESHOLD = 0.5;

export type MetricEmphasisContext = {
  parentTotalCost?: Decimal;
  parentTotalDurationMs?: number;
};

function nodeDurationMs(node: TreeNode): number | undefined {
  if (node.endTime) return node.endTime.getTime() - node.startTime.getTime();
  if (node.latency != null) return node.latency * 1000;
  return undefined;
}

/**
 * Undefined for the trace root and for an only child: both are always 100% of
 * their parent, so a share tells the reader nothing.
 */
export function resolveMetricEmphasisContext(
  node: TreeNode,
  nodeMap: Map<string, TreeNode>,
  roots: TreeNode[],
): MetricEmphasisContext | undefined {
  if (node.type === "TRACE") return undefined;
  const parent = node.parentObservationId
    ? nodeMap.get(node.parentObservationId)
    : roots.find((root) => root.type === "TRACE");
  const siblings = parent ? parent.children : roots;
  if (siblings.length < 2) return undefined;
  if (parent) {
    return {
      parentTotalCost: parent.totalCost,
      parentTotalDurationMs: nodeDurationMs(parent),
    };
  }
  // Events-based traces with several roots: compare against all of them.
  const durations = siblings
    .map(nodeDurationMs)
    .filter((d): d is number => d != null);
  return {
    parentTotalCost: siblings.reduce<Decimal | undefined>((acc, s) => {
      if (!s.totalCost) return acc;
      return acc ? acc.plus(s.totalCost) : s.totalCost;
    }, undefined),
    parentTotalDurationMs:
      durations.length > 0 ? Math.max(...durations) : undefined,
  };
}

export function isEmphasizedShare(
  value: number | Decimal | undefined,
  total: number | Decimal | undefined,
): boolean {
  if (value == null || total == null) return false;
  const totalDecimal = new Decimal(total);
  if (totalDecimal.lte(0)) return false;
  return new Decimal(value).div(totalDecimal).gte(METRIC_EMPHASIS_THRESHOLD);
}
