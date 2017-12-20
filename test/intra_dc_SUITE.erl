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
    replication_test/1
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
    replication_test
].

replication_test(Config) ->
    lager:info("Replication test starting."),
    Cluster = proplists:get_value(clusters, Config),
    Clusters = lists:map(fun(Node) ->
        rpc:call(Node, intra_dc_leader_elector, get_cluster, [])
    end, Cluster),
    [ClusterState | _] = Clusters,

    % Check that groups are consistent
    ?assertEqual(lists:all(fun(X) -> X == ClusterState end, Clusters), true),

    % Apply write
    Key1 = simple_replication_test_dc,
    Type = antidote_crdt_counter,
    Bucket = intradc_test,
    WriteResult1 = rpc:call(hd(Cluster),
                            antidote, update_objects,
                            [ignore, [], [{{Key1, Type, Bucket}, increment, 1}]]),
    ?assertMatch({ok, _}, WriteResult1),

    % Kill a node
    test_utils:kill_nodes([hd(Cluster)]),
    ct:sleep(1000),
    Clusters2 = lists:map(fun(Node) ->
        rpc:call(Node, intra_dc_leader_elector, get_cluster, [])
    end, tl(Cluster)),
    [ClusterState2 | _] = Clusters2,
    ?assertEqual(lists:all(fun(X) -> X == ClusterState2 end, Clusters2), true),


    % Apply read
    {ok, [ReadResult], _} = rpc:call(hd(tl(Cluster)), antidote, read_objects,
                          [ignore, [], [{Key1, Type, Bucket}]]),
    ?assertEqual(1, ReadResult),

    % Recover
    test_utils:restart_nodes([hd(Cluster)], Config),
    ct:sleep(1000),

    Clusters3 = lists:map(fun(Node) ->
        rpc:call(Node, intra_dc_leader_elector, get_cluster, [])
    end, Cluster),
    [ClusterState3 | _] = Clusters3,
    ?assertEqual(lists:all(fun(X) ->
        case X == ClusterState3 of
            true -> true;
            false ->
                ct:print("~p, ~p~n", [X, ClusterState3]),
                false
        end
    end, Clusters3), true),

    lager:info("Replication test passed."),
    pass.
