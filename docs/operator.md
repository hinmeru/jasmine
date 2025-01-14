# Operator

## add(`+`)

|           | bool   | int    | float  | string | cat    | date   | time   | datetime | timestamp | duration  |
| --------- | ------ | ------ | ------ | ------ | ------ | ------ | ------ | -------- | --------- | --------- |
| bool      | int    | int    | float  | string | -      | -      | -      | -        | -         | -         |
| int       | int    | int    | float  | string | -      | -      | -      | -        | -         | -         |
| float     | float  | float  | float  | string | -      | -      | -      | -        | -         | -         |
| string    | string | string | string | string | string | string | string | string   | string    | string    |
| cat       | -      | -      | -      | string | -      | -      | -      | -        | -         | -         |
| date      | -      | -      | -      | string | -      | -      | -      | -        | -         | timestamp |
| time      | -      | -      | -      | string | -      | -      | -      | -        | -         | -         |
| datetime  | -      | -      | -      | string | -      | -      | -      | -        | -         | duration  |
| timestamp | -      | -      | -      | string | -      | -      | -      | -        | -         | timestamp |
| duration  | -      | -      | -      | string | -      | date   | -      | datetime | timestamp | duration  |
