//go:build !linux

// POSIX message queues exist only on the charger (Linux). Elsewhere these stubs let
// the bridge build and its tests run on a development machine.

package wallbox

func mqOpen(path []byte) uintptr                  { return 0 }
func mqTimedsend(fd uintptr, data []byte) uintptr { return 0 }
func mqClose(fd uintptr)                          {}
