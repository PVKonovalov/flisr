# FLISR

FLISR - fault location, isolation, and service restoration service can automate power restoration in seconds,
automatically isolating faults and restoring power to disconnected feeders.


## Configuration 
All FLISR configuration is stored in .ini file.
```ini
[FLISR]
; Logging level: ERROR, WARN, INFO, DEBUG
LOG = DEBUG

; Automatic circuit recloser delay
ARC_DELAY_SEC = 6

; Length of internal communication queue
QUEUE = 100

; FLISR state SCADA tag identifier
RTDB_POINT_FLISR_STATE = 2887

; FLISR tag source identifier
POINT_SOURCE = 15
```
