package queryrows

// CompletedTraceLimit is the deepest ranking CompletedTraceQuery answers
// exactly; the read cache keeps enough spans per partition for this depth.
const CompletedTraceLimit = 21

// CompletedTraceQuery ranks scoped traces by their best in-window span: an
// error first, then the longest span, then trace_id and namespace. Arguments
// are start, end, namespace twice, service twice, and a limit of at most
// CompletedTraceLimit. Results are namespace, trace_id, and whether the trace
// has an in-scope, in-window error span.
const CompletedTraceQuery = `SELECT namespace,trace_id,max(has_error) AS has_error FROM trace_exemplars
 WHERE start_time>=$1::TIMESTAMP_NS::TIMESTAMPTZ_NS AND start_time<$2::TIMESTAMP_NS::TIMESTAMPTZ_NS
 AND ($3='' OR namespace=$4) AND ($5='' OR service=$6) AND trace_id<>''
 GROUP BY namespace,trace_id
 ORDER BY max([has_error::BIGINT,duration]) DESC,trace_id,namespace LIMIT $7`
