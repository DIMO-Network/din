package s3client

import (
	"github.com/DIMO-Network/din/internal/objstore"
	"github.com/DIMO-Network/din/internal/split"
)

// Verify Client implements the shared store surface and the splitter's blob
// store. Both interfaces are defined next to their consumers; these
// compile-time checks keep Client in lockstep with them.
var (
	_ objstore.Store    = (*Client)(nil)
	_ split.ObjectStore = (*Client)(nil)
)
