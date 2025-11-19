read -r -d '' VAR <<- EOM
LOAD airport;
CREATE OR REPLACE SECRET airport_autogluon (
    type AIRPORT,
    auth_token 'abc123',
    scope 'grpc://127.0.0.1:8080'
);
ATTACH 'hello2' (TYPE AIRPORT, location 'grpc://127.0.0.1:8080');
SELECT database, schema, name FROM (show all tables);
EOM


duckdb -c "$VAR"