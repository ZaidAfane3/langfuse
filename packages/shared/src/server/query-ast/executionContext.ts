/**
 * Compile-time tenancy scope. Every ClickHouse query compiled through
 * {@link compileClickhouseQuery} must carry one of these; the tenancy
 * injection pass keys off the scope.
 *
 * Single-project reads use `projectId` (equality). Org-level scans that are
 * already authorized across a known set use `projectIds` (`IN`).
 */
export type ExecutionContext =
  | { projectId: string; projectIds?: undefined }
  | { projectId?: undefined; projectIds: readonly string[] };
