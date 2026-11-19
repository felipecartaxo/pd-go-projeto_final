package pubsub

import "github.com/felipecartaxo/pd-go-projeto_final/internal/control"

// ErrInconclusive means a bounded control exchange received no confirmation.
// It does not say whether the broker applied the requested operation.
var ErrInconclusive = control.ErrInconclusive
