read -r -d '' VAR <<- EOM
LOAD airport;
CREATE OR REPLACE SECRET airport_autogluon (
    type AIRPORT,
    auth_token 'teadaast',
    scope 'grpc://127.0.0.1:8080'
);

ATTACH 'hello2' (TYPE AIRPORT, location 'grpc://127.0.0.1:8080');

--USE hello3.TEst;
SELECT database, schema, name FROM (show all tables);

SELECT date FROM hello2.dremioproduction."APEX.Orbit.usage.dashboard_activity" LIMIT 100;
--SELECT * FROM "APEX.Orbit.usage.community_tags"  LIMIT 100;

--SELECT * FROM airport_take_flight('grpc://localhost:8080/', 'SELECT * FROM dremioproduction."APEX.Orbit.usage.code_promotions" LIMIT 1000');

--select COUNT(*) from hello.dremio."APEX.APEX_Reflections"
--select * from hello.dremio."APEX.APEX_Reflections"
---select a.id, b.id, a.username, b.username from hello.dremio."APEX.Orbit.usage.code_promotions" a LEFT JOIN hello.dremio."APEX.Orbit.usage.code_promotions" b ON a.id = b.id WHERE a.id = 1 LIMIT 1000
--select * from hello.dremio."APEX.Orbit.cleansed.dqcs.app_maersk_firebaseevent__spotconfirm" WHERE action = 'aaa'
EOM

    
duckdb -c "$VAR"