#!/usr/bin/env bash
# Copyright 2019 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

TIDB_TEST_STORE_NAME=$TIDB_TEST_STORE_NAME
TIKV_PATH=$TIKV_PATH

build=0
mysql_tester="./mysql_tester"
portgenerator=""
mysql_tester_log="./integration-test.out"
tests="expression/vector"
record=0
record_case=""
stats="s"
collation_opt=2

set -eu
trap 'set +e; PIDS=$(jobs -p); [ -n "$PIDS" ] && kill -9 $PIDS' EXIT
# make tests stable time zone wise
export TZ="Asia/Shanghai"

function help_message()
{
    echo "Usage: $0 [options]

    -h: Print this help message.

    -d <y|Y|n|N|b|B>: \"y\" or \"Y\" for only enabling the new collation during test.
                      \"n\" or \"N\" for only disabling the new collation during test.
                      \"b\" or \"B\" for tests the prefix is `collation`, enabling and disabling new collation during test, and for other tests, only enabling the new collation [default].
                      Enable/Disable the new collation during the integration test.

    -s <tidb-server-path>: Use tidb-server in <tidb-server-path> for testing.
                           eg. \"./run-tests.sh -s ./integrationtest_tidb-server\"

    -b <y|Y|n|N>: \"y\" or \"Y\" for building test binaries [default \"y\" if this option is not specified].
                  \"n\" or \"N\" for not to build.
                  The building of tidb-server will be skiped if \"-s <tidb-server-path>\" is provided.
                  The building of portgenerator will be skiped if \"-s <portgenerator-path>\" is provided.

    -r <test-name>|all: Run tests in file \"t/<test-name>.test\" and record result to file \"r/<test-name>.result\".
                        \"all\" for running all tests and record their results.

    -t <test-name>: Run tests in file \"t/<test-name>.test\".
                    This option will be ignored if \"-r <test-name>\" is provided.
                    Run all tests if this option is not provided.

    -p <portgenerator-path>: Use port generator in <portgenerator-path> for generating port numbers.

"
}

function extract_stats()
{
    echo "extracting statistics: $stats"
    rm -rf $stats
    unzip -qq s.zip
}

while getopts "t:s:r:b:d:c:i:h:p" opt; do
    case $opt in
        t)
            tests="$OPTARG"
            ;;
        s)
            tidb_server="$OPTARG"
            ;;
        r)
            record=1
            record_case="$OPTARG"
            ;;
        b)
            case $OPTARG in
                y|Y)
                    build=1
                    ;;
                n|N)
                    build=0
                    ;;
                *)
                    help_message 1>&2
                    exit 1
                    ;;
            esac
            ;;
        d)
            case $OPTARG in
                y|Y)
                    collation_opt=1
                    ;;
                n|N)
                    collation_opt=0
                    ;;
                b|B)
                    collation_opt=2
                    ;;
                *)
                    help_message 1>&2
                    exit 1
                    ;;
            esac
            ;;
        h)
            help_message
            exit 0
            ;;
        p)  
            portgenerator="$OPTARG"
            ;;
        *)
            help_message 1>&2
            exit 1
            ;;
    esac
done

extract_stats

rm -rf $mysql_tester_log

#ports=()
#for port in $($portgenerator -count 2); do
#    ports+=("$port")
#done

port=4000
status=10080

function run_mysql_tester()
{
    coll_disabled="false"
    coll_msg="enabled new collation"
    if [[ $enabled_new_collation = 0 ]]; then
        coll_disabled="true"
        coll_msg="disabled new collation"
    fi
    if [ $record -eq 1 ]; then
      if [ "$record_case" = 'all' ]; then
          echo "record all cases"
          $mysql_tester -port "$port" --check-error=true --collation-disable=$coll_disabled --record
      else
          echo "record result for case: \"$record_case\""
          $mysql_tester -port "$port" --check-error=true --collation-disable=$coll_disabled --record $record_case
      fi
    else
      if [ -z "$tests" ]; then
          echo "run all integration test cases ($coll_msg)"
      else
          echo "run integration test cases($coll_msg): $tests"
      fi
      $mysql_tester -port "$port" --check-error=true --collation-disable=$coll_disabled $tests
    fi
}

function check_data_race() {
    if [ "${TIDB_TEST_STORE_NAME}" = "tikv" ]; then
        return
    fi
    race=`grep 'DATA RACE' $mysql_tester_log || true`
    if [ ! -z "$race" ]; then
        echo "tidb-server DATA RACE!"
        cat $mysql_tester_log
        exit 1
    fi
}

enabled_new_collation=""
function check_case_name() {
    if [ $collation_opt != 2 ]; then
        return
    fi

    case=""

    if [ $record -eq 0 ]; then
        if [ -z "$tests" ]; then
            return
        fi
        case=$tests
    fi

    if [ $record -eq 1 ]; then
        if [ "$record_case" = 'all' ]; then
            return
        fi
        case=$record_case
    fi

    IFS='/' read -ra parts <<< "$case"

    last_part="${parts[${#parts[@]}-1]}"

    if [[ $last_part == collation* || $tests == collation* ]]; then
        collation_opt=2
    else
        collation_opt=1
    fi
}

check_case_name
if [[ $collation_opt = 0 || $collation_opt = 2 ]]; then
    enabled_new_collation=0
    # start_tidb_server
    run_mysql_tester
#    kill -15 $SERVER_PID
#    while ps -p $SERVER_PID > /dev/null; do
#        sleep 1
#    done
    check_data_race
fi

if [[ $collation_opt = 1 || $collation_opt = 2 ]]; then
    enabled_new_collation=1
    # start_tidb_server
    run_mysql_tester
#    kill -15 $SERVER_PID
#    while ps -p $SERVER_PID > /dev/null; do
#        sleep 1
#    done
    check_data_race
fi

echo "integrationtest passed!"
