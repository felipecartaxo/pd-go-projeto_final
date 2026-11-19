package pubsub

import "github.com/Neraky/pd-go-cenario-2-implementacao/internal/control"

// ErrInconclusive means a bounded control exchange received no confirmation.
// It does not say whether the broker applied the requested operation.
var ErrInconclusive = control.ErrInconclusive
