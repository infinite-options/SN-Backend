#!/bin/bash
# Type password when prompted to access the MySQL console for Infinite Options RDS

function usage () {
    echo "usage:";
    echo "--login, -l: log onto mysql server"
    echo "--run-schema, -r: setup schema in database, use with caution";
}

function logon() {
    mysql -h pm-mysqldb.cxjnrciilyjq.us-west-1.rds.amazonaws.com -u admin -D pricing -p
}

if [[ $# -eq 0 ]]; then
    usage;
    logon;
    exit 0
elif [[ $# -eq 1 ]]; then
    case $1 in
    -l | --login)
        logon;
        exit 0
        ;;
    -r | --run-schema)
        # Must run within same directory as schema.sql
        mysql -h pm-mysqldb.cxjnrciilyjq.us-west-1.rds.amazonaws.com -u admin -D pricing -p < schema.sql
        exit 0
        ;;
    *)
        usage
        exit 1
        ;;
    esac
else
    usage;
fi

