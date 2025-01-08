# Temporal

## Built-in Functions

- `tz.convert(datetime, tz)`

underlying UTC timestamp is not changed

```jasmine
df0 = df[time = [2024-12-10T10:00:00]]
update tokyo_time = time ~tz.convert `Asia/Tokyo from df0
```

- `tz.replace(datetime, tz)`

update underlying timestamp

```jasmine
df0 = df[time = [2024-12-10T10:00:00]]
update tokyo_time = time ~tz.replace `Asia/Tokyo from df0
```
