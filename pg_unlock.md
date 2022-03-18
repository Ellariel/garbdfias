1. Этот запрос показывает блокировки таблиц (locks) - активные (granted = t) и ждущие (granted = f):

```sql
SELECT t.schemaname,
    t.relname,
    l.locktype,
    l.page,
    l.virtualtransaction,
    l.pid,
    l.mode,
    l.granted
   FROM pg_locks l
   JOIN pg_stat_all_tables t ON l.relation = t.relid
  WHERE t.schemaname <> 'pg_toast'::name AND t.schemaname <> 'pg_catalog'::name
  ORDER BY t.schemaname, t.relname;
```

Пример вывода:

```
 schemaname |       relname       | locktype | page | virtualtransaction |  pid  |        mode         | granted 
------------+---------------------+----------+------+--------------------+-------+---------------------+---------
 cat        | KIM_Params_comments | relation |      | 58/109131          |  3895 | AccessShareLock     | f
 cat        | KIM_Params_comments | relation |      | 55/214191          |  3892 | AccessShareLock     | f
 cat        | KIM_Params_comments | relation |      | 30/72487           |  3844 | AccessShareLock     | f
 cat        | KIM_Params_comments | relation |      | 23/759234          | 32694 | RowExclusiveLock    | t
 cat        | KIM_Params_comments | relation |      | 59/431036          |  3875 | AccessExclusiveLock | f
 cat        | KIM_sShipsWay       | relation |      | 23/759234          | 32694 | AccessShareLock     | t
 cat        | KIM_sShipsWay_load  | relation |      | 23/759234          | 32694 | AccessShareLock     | t
```

2. Для `granted = t` смотрим колонку `pid` - id процесса, создавшего лок; здесь это 32694 (RowExclusiveLock на таблице `KIM_Params_comments`). Смотрим инфу о процессе:

```sql
select * from pg_stat_activity where pid in (32694);
```

3. В частности нас интересует `status` - если он `idle in transaction`, значит, завис) пробуем выполнить

```sql
SELECT pg_cancel_backend(32694);
```

Если не помогло,

```sql
SELECT pg_terminate_backend(32694);
```

Относительно прав на выполнение этих команд: юзер должен принадлежать той же роли, что и процесс, либо иметь привилегию `pg_signal_backend`. Процессы суперпользователя могут завершать только суперы

----

Источники

* [еще один алгоритм решения проблемы](https://stackoverflow.com/a/20374942/1780443)
* [запрос, показывающий активные блокировки](https://stackoverflow.com/a/23060492/1780443)
* [о разнице между `pg_cancel_backend` и `pg_terminate_backend`](https://serverfault.com/a/35344/152991)
* [документация по `pg_cancel_backend` и `pg_terminate_backend`](https://www.postgresql.org/docs/current/static/functions-admin.html)
* [о привилегии `pg_signal_backend`](https://postgrespro.ru/docs/postgresql/9.6/default-roles.html)
