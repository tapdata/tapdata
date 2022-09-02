## Connection configuration help

### Parameter Description

- `Initial data volume`: The total number of data to be fully synchronized, range: `0 ~ 9223372036854775807`.
- `Increment Interval + Total Increment Intervals`: Controls the frequency of incremental data generation, indicating that `Increment Intervals` pieces of data are generated within the `increment interval` time, range: `0 ~ 2147483647`.
- `Incremental event type`: Indicates which event types are generated. If all three are checked, the `insert event` is generated first, then the `update event` is generated, and finally the `delete event` is generated, and the cycle is repeated in this order until the end.
- `Write Interval + Total Write Interval`: Used as a target to control the frequency of consuming events. Indicates the `total number of write intervals` pieces of data consumed within the `write interval`, range: `0 ~ 2147483647`.
- `Print Write Log`: When on, output received event data.
- `table name + field definition`: used to define the data model as a source.

### Model

#### Field Type

- `string[($byte)][fixed]`: string
  - `$byte`: length in bytes (default: `64`)
  - `fixed`: if fixed-length characterizer is added with this flag (default: non-fixed length)
- `number[($precision,$scale)]`: numeric value
  - `$precision`: length (range `1-40`, default `4`)
  - `$scale`: precision (range `0-10`, default `1`)
- `boolean`: boolean value
- `date`: the date
- `array`: array
- `binary`: bytes
- `map`: key-value pair
- `time`: time
- `datetime`: date + time
- `now`: current time
- `uuid`: UUID
- `serial[($begin,$step)]`: auto increment
  - `$begin`: starting position (default: `1`)
  - `$step`: step size (default: `1`)
- `rnumber[($precision)]`: random number
  - `$precision`: length (default: `4`)
- `rstring[($byte)]`: random characters of specified length
  - `$byte`: length in bytes (default: `64`)

#### Field default values

The default value used when generating data, the data is `null` if not set

### illustrate
> When `mode='ConnHeartbeat'`, other parameters do not need to be configured:
> - as source only
> - no data at all
> - has a fixed data model:
> ````
> _tapdata_heartbeat_table=[
> { "type": "string(64)", "pri": true, "name": "id", "def": "$connId" },
> { "type": "now", "pri": false, "name": "ts" }
> ]
> ````
> - Fixed frequency: `1 bar/1000ms`
> - only generate update events