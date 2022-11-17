# FLISR

FLISR - fault location, isolation, and service restoration service can automate power restoration in seconds,
automatically isolating faults and restoring power to disconnected feeders.


## Configuration 
All FLISR configuration is stored in .ini file.
```ini
[FLISR]
; Logging level: ERROR, WARN, INFO, DEBUG
LOG = DEBUG

; Path to state machine configuration
STATE_MACHINE_CONFIG = state_machine.yml

; Length of internal queue
QUEUE = 100

; FLISR state RTDB point identifier from POINT table
RTDB_POINT_FLISR_STATE = 2887

; FLISR point source from POINT_SOURCE table
POINT_SOURCE = 15
```
## State machine diagram
The diagram describes how the result of the auto-reclosing process is determined.
![State machine diagram](assets/StateMachineDiagram.png)

```
On  - The switch turns on
Off - The switch turns off
PR  - Overcurrent protection alarm
RC  - Automatic reclosing in progress
T   - Go to next state by timeout
```