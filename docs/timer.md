# Timer

## Built-in functions

### task

list all tasks

```
task()
```

### schedule

schedule a task

```
schedule(function, args, start_time, end_time, interval, description)
```

### trigger

trigger a task, set next run time to now

```
trigger(task_id)
```

### unpause

unpause a task, set is_active to True

```
unpause(task_id)
```

### pause

pause a task, set is_active to False

```
pause(task_id)
```
