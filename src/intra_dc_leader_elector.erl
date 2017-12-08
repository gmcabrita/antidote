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

-module(intra_dc_leader_elector).
-behaviour(gen_server).

%% External API
-export([
    ring_changed/0,
    get_cluster/0,
    get_cluster/1,
    get_preflist/1
]).

%% Internal API
-export([
    init/1,
    start_link/0,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% GenServer state
-record(state, {
    partitions :: map() %% map<partition(), cluster()>
}).

%%%% External API

ring_changed() ->
    gen_server:cast({global, generate_server_name(node())}, ring_changed).

get_cluster() ->
    gen_server:call({global, generate_server_name(node())}, get_cluster).

get_cluster(Partition) ->
    gen_server:call({global, generate_server_name(node())}, {get_cluster, Partition}).

get_preflist(Partition) ->
    gen_server:call({global, generate_server_name(node())}, {get_preflist, Partition}).

%%%% Internal API

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({global, generate_server_name(node())}, ?MODULE, [], []).

init([]) ->
    {ok, #state{partitions = recompute_groups(1)}}.

handle_call(get_cluster, _From, #state{partitions = Partitions} = State) ->
    {reply, Partitions, State};
handle_call({get_cluster, Partition}, _From, #state{partitions = Partitions} = State) ->
    {reply, maps:get(Partition, Partitions), State};
handle_call({get_preflist, Partition}, _From, #state{partitions = Partitions} = State) ->
    {reply, maps:get(membership, maps:get(Partition, Partitions)), State};
handle_call(_Info, _From, State) ->
    {reply, error, State}.

handle_cast(ring_changed, State) ->
    {noreply, State#state{partitions = recompute_groups(3)}};
handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%% Private

recompute_groups(N) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    AllPrefs = riak_core_ring:all_preflists(Ring, N), % TODO: @gmcabrita size is hardcoded for now
    lists:foldl(fun([{Partition, _} | _] = Membership, Acc) ->
        Map = #{
            membership => Membership
        },
        maps:put(Partition, Map, Acc)
    end, #{}, AllPrefs).

%% Generates a server name from a given node name.
generate_server_name(Node) ->
    list_to_atom("intra_dc_leader_elector" ++ atom_to_list(Node)).
