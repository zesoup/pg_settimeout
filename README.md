# pg_settimeout
An adaption of the setTimeout(fnc* , timeout) function of javascript.
(tested with pg9.6)

It basically takes an SQL statement of any kind and relays the task to a backgroundworker.
Usefull for some implementations that would otherwise require an application-level mechanism.

Provides two functions:


* pid pg_settimeout (query, timeout) : will execute the query after <timeout>ms .
* pid pg_setinterval(query, timeout) : will do the same, but repeat the task forever.


Still in Development

## Example - Timeoutception


    CREATE EXTENSION  pg_settimeout;

    CREATE TABLE loghere(id serial primary key, pid int, status text);

    CREATE OR REPLACE FUNCTION nonstop() RETURNS integer AS $function$
         BEGIN
                INSERT INTO loghere(pid, status) 
                VALUES ( (select pg_settimeout(
                          $$
                              UPDATE loghere 
                              SET status = 'DONE' 
                              WHERE pid=pg_backend_pid() ;      
                          $$,1000)), 'planned');
 
                PERFORM pg_settimeout(' select nonstop() ', 5000);
                RETURN 0;
        END;
    $function$ LANGUAGE plpgsql;


    SELECT nonstop();


Now observe the loghere table or pg_stat_activity. It should look something like this:


     id |  pid  | status  
    ----+-------+---------
      1 | 15255 | DONE
      2 | 15257 | DONE
      3 | 15259 | DONE
      4 | 15261 | DONE
      5 | 15263 | planned


## Future Updates:
- C-Level mechanisms to ensure an interval isnt delayed by executiontime.
