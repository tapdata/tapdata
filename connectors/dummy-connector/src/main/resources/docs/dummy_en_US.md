## Connection configuration help

### Parameter Description

- `Initialized data volume`: The total number of data to be fully synchronized.
- `Increment Interval + Total Increment Intervals`: Controls the frequency of incremental data generation, indicating that `Increment Intervals` pieces of data are generated within the `Increment Interval` period.
- `Incremental event type`: Indicates which event types are generated. If all three are checked, the `insert event` is generated first, then the `update event` is generated, and finally the `delete event` is generated, and the cycle is repeated in this order until the end.
- `Write Interval + Total Write Interval`: Used as a target to control the frequency of consuming events. Indicates that `total number of write intervals` pieces of data are consumed within the `write interval` time.
- `Print Write Log`: When on, output received event data.
- `table name + field definition`: used to define the data model as a source.

### Model

#### Field Type

- `String[($byte)][fixed]`: String.
    - `$byte`: length in bytes
    - `fixed`: if the fixed-length characterizer is added with this flag
- `Number[($precision, $scale)]`: Number.
    - `$precision`: range `1-40`, default `4`
    - `$scale`: range `0-10`, default `1`
- `Boolean`: boolean value
- `Date`: the date
- `Array`: array
- `Binary`: bytes
- `Map`: key-value pair
- `Time`: time
- `DateTime`: date + time

#### Field default values

The default value used when generating data, the data is `null` when not set, supporting functions:

- `serial(int,int)`: increment, start position, step size
- `now()`: current time
- `randomLong(long)`: random number
- `randomString(int)`: random character of specified length
- `uuid()`: UUID

### illustrate
> When `mode='ConnHeartbeat'`, other parameters do not need to be configured:
> - As source only
> - No data at all
> - Has a fixed data model:
> ````
> _tapdata_heartbeat_table=[
> { "type": "String(64)", "pri": true, "name": "id", "def": "$connId" },
> { "type": "DateTime", "pri": false, "name": "ts", "def": "now()" }
> ]
> ````
> - Fixed frequency: `1 bar/1000ms`
> - Only generate update events