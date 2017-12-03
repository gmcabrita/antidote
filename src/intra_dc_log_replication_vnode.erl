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

%% This vnode is responsible for coordinating the replication of log
%% operations between different Antidote nodes.

%% TODO: @gmcabrita
%% - rewrite with new logic in mind

-module(intra_dc_log_replication_vnode).
-behaviour(riak_core_vnode).
-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

%% External API
-export([
    replicate/2
]).

%% Internal API
-export([
    init/1,
    start_vnode/1,
    handle_command/3,
    handle_coverage/4,
    handle_exit/3,
    handoff_starting/2,
    handoff_cancelled/1,
    handoff_finished/2,
    handle_handoff_command/3,
    handle_handoff_data/2,
    encode_handoff_item/2,
    is_empty/1,
    terminate/2,
    delete/1]).

%% VNode state
-record(state, {
    partition :: partition_id()
}).

%%%% External API

replicate(Partition, Buffer) ->
    riak_core_vnode_master:sync_command({Partition, node()}, {txn, Partition, Buffer}, intra_dc_log_replication_vnode_master).

%%%% Internal API

start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, #state{partition = Partition}}.

handle_command({txn, OriginalPartition, Buffer}, _From, State) ->
    %% TODO: @gmcabrita, consider storing this locally and updating whenever it actually changes
    Cluster = intra_dc_leader_elector:get_cluster(OriginalPartition),
    [_Leader, TargetNode | TargetRemainingNodes] = maps:get(membership, Cluster),
    %% TODO: @gmcabrita, retry in case the call fails
    ok = riak_core_vnode_master:sync_command(TargetNode, {run_txn, OriginalPartition, TargetRemainingNodes, Buffer}, intra_dc_log_replication_vnode_master),
    {reply, ok, State};

handle_command({run_txn, OriginalPartition, RemainingNodes, Buffer}, _From, State) ->
    Log = open_log(OriginalPartition),
    lists:map(fun(LogRecord) -> disk_log:log(Log, {[OriginalPartition], LogRecord}) end, Buffer),
    lager:info("Node: ~p, replicated correctly for original partition ~p~n", [node(), OriginalPartition]),
    Result = case RemainingNodes == [] of
        true -> ok;
        false ->
            %% TODO: @gmcabrita, we use 1 here because we're assuming we always have N=3
            %% If N ever changes, the sync_command has to be adjusted too, as the quorum will increase
            %% so the call will need to be synchronous for more nodes down the chain
            NextBuffer = case length(RemainingNodes) == 1 of
                true -> filter_buffer(Buffer);
                false -> Buffer
            end,
            [TargetNode | TargetRemainingNodes] = RemainingNodes,
            spawn(fun() -> riak_core_vnode_master:sync_command(TargetNode, {run_txn, OriginalPartition, TargetRemainingNodes, NextBuffer}, intra_dc_log_replication_vnode_master) end),
            ok
    end,
    {reply, Result, State};

handle_command({hello}, _Sender, State) ->
    {reply, ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.
handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.
handoff_starting(_TargetNode, State) ->
    {true, State}.
handoff_cancelled(State) ->
    {ok, State}.
handoff_finished(_TargetNode, State) ->
    {ok, State}.
handle_handoff_command( _Message , _Sender, State) ->
    {noreply, State}.
handle_handoff_data(_Data, State) ->
    {reply, ok, State}.
encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).
is_empty(State) ->
    {true, State}.
delete(State) ->
    {ok, State}.
terminate(_Reason, _State) ->
    ok.

%%%% Private Functions

open_log(Partition) ->
    LogFile = integer_to_list(Partition),
    LogId = LogFile ++ "--" ++ LogFile,
    LogPath = filename:join(app_helper:get_env(riak_core, platform_data_dir), LogId),
    case disk_log:open([{name, LogPath}]) of
        {ok, Log} -> Log;
        {repaired, Log, _, _} -> Log;
        {error, Reason} -> {error, Reason}
    end.

filter_buffer(Buffer) ->
    B = lists:foldl(fun(LogRecord, Acc) ->
        Operation = LogRecord#log_record.log_operation,
        case Operation#log_operation.op_type of
            update ->
                Update = Operation#log_operation.log_payload,
                Type = Update#update_log_payload.type,
                Op = Update#update_log_payload.op,
                case antidote_ccrdt:is_type(Type) of
                    true ->
                        case Type:is_replicate_tagged(Op) of
                            true -> Acc;
                            false -> [LogRecord | Acc]
                        end;
                    false -> [LogRecord | Acc]
                end;
            _ -> [LogRecord | Acc]
        end
    end, [], Buffer),
    case length(B) > 2 of
        true -> lists:reverse(B);
        false -> []
    end.
