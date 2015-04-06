# pg_settimeout
POC for an adaption of the setTimeout(fnc* , timeout) function of javascript.
(tested with pg9.4)

It basically takes an SQL statement of any kind and relays the task to a backgroundworker.
Usefull for some implementations that would otherwise require an application-level mechanism.

Currently misses many things, e.G. a proper overview of the running processes or following the 
executing process to a database (it's currently hardcoded to postgres).

## Example Usage:


    CREATE EXTENSION  pg_settimeout;

    CREATE TABLE loghere(id serial primary key, pid int, status text);

    CREATE OR REPLACE FUNCTION nonstop() RETURNS integer AS $function$
         BEGIN
                INSERT INTO loghere(pid, status) VALUES ( (select pg_settimeout(
                $$
                UPDATE loghere SET status = 'DONE' WHERE pid=pg_backend_pid() ;      
                $$,10)), 'planned');
                
                PERFORM pg_settimeout(' select nonstop() ', 5);
                RETURN 1;
        END;
    $function$ LANGUAGE plpgsql;


    SELECT nonstop();


Now observe the loghere Table.
