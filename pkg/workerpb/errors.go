package workerpb

// Details the engine attaches to UNAVAILABLE as a google.rpc.ErrorInfo (ADR-029).
const (
	ErrorDomain = "jobscheduler"
	// ReasonDatabaseUnavailable means the engine couldn't reach the database; metadata
	// MetadataNodeID names the engine.
	ReasonDatabaseUnavailable = "DATABASE_UNAVAILABLE"
	MetadataNodeID            = "node_id"
)
