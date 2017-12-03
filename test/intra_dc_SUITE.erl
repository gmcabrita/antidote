%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(intra_dc_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([
    %% suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0
]).

%% tests
-export([
    leader_test/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").


init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Cluster = test_utils:set_up_dc_common(Config),

    [{clusters, Cluster}|Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

all() -> [
    leader_test
].

leader_test(Config) ->
    lager:info("Leader test starting."),
    Cluster = proplists:get_value(clusters, Config),
    Leaders = lists:map(fun(Node) ->
        ct:print("~p~n", [rpc:call(Node, log_utilities, get_preflist_from_key, [1])]),
        ct:print("~p~n", [rpc:call(Node, log_utilities, get_preflist_from_key, [2])]),
        rpc:call(Node, intra_dc_leader_elector, get_leader, [])
    end, Cluster),
    [Leader | _] = Leaders,
    %% note: we should probably check for majority instead
    ?assertEqual(lists:duplicate(3, Leader), Leaders),
    test_utils:kill_nodes([Leader]),
    ct:sleep(1000),
    ct:print("Initial leader was: ~p~n", [Leader]),
    Leaders2 = lists:map(fun(Node) ->
        ct:print("~p~n", [rpc:call(Node, log_utilities, get_preflist_from_key, [1])]),
        ct:print("~p~n", [rpc:call(Node, log_utilities, get_preflist_from_key, [2])]),
        rpc:call(Node, intra_dc_leader_elector, get_leader, [])
        end, Cluster -- [Leader]),
    [Leader2 | _] = Leaders2,
    %% note: we should probably check for majority instead
    ?assertEqual(lists:duplicate(2, Leader2), Leaders2),
    lager:info("Leader test passed."),
    pass.
