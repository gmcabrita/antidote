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
    partition :: partition_id(),
    last_ops :: map()
}).

%%%% External API

replicate(Partition, Buffer) ->
    riak_core_vnode_master:sync_command({Partition, node()}, {txn, Partition, Buffer}, intra_dc_log_replication_vnode_master).

%%%% Internal API

start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, #state{partition = Partition, last_ops = #{Partition => last_op_from_disk(Partition)}}}.

handle_command({txn, OriginalPartition, Buffer}, _From, State) ->
    txn(OriginalPartition, Buffer, State);

handle_command({run_txn, OriginalPartition, RemainingNodes, Buffer, OpNumber}, _From, State = #state{last_ops = CurrentOps}) ->
    CurrentOp = case maps:is_key(OriginalPartition, CurrentOps) of
        true -> maps:get(OriginalPartition, CurrentOps);
        false -> last_op_from_disk(OriginalPartition)
    end,
    case OpNumber > CurrentOp of
        true -> {reply, {missing, CurrentOp}, State};
        false -> run_txn(OriginalPartition, RemainingNodes, Buffer, OpNumber, State)
    end;

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

txn(OriginalPartition, Buffer, State = #state{last_ops = CurrentOps}) ->
    CurrentOp = maps:get(OriginalPartition, CurrentOps),
    Cluster = intra_dc_leader_elector:get_cluster(OriginalPartition),
    [_Leader, TargetNode | TargetRemainingNodes] = maps:get(current, Cluster),
    %% TODO: @gmcabrita, retry in case the call fails
    case riak_core_vnode_master:sync_command(TargetNode, {run_txn, OriginalPartition, TargetRemainingNodes, Buffer, CurrentOp}, intra_dc_log_replication_vnode_master) of
        ok -> ok;
        {missing, _Number} ->
            % TODO: @gmcabrita, read 'OldBuffer' from log from Number till CurrentOp
            % Send OldBuffer, after ok send Buffer
            ok
    end,
    LastRecord = lists:last(Buffer),
    LastOp = LastRecord#log_record.op_number#op_number.local,
    {reply, ok, State#state{last_ops = maps:put(OriginalPartition, LastOp, CurrentOps)}}.

run_txn(OriginalPartition, RemainingNodes, Buffer, CurrentOp, State = #state{last_ops = CurrentOps}) ->
    Log = open_log(OriginalPartition),
    lists:map(fun(LogRecord) -> disk_log:log(Log, {[OriginalPartition], LogRecord}) end, Buffer),
    case RemainingNodes == [] of
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

            spawn(fun() ->
                case riak_core_vnode_master:sync_command(TargetNode, {run_txn, OriginalPartition, TargetRemainingNodes, NextBuffer, CurrentOp}, intra_dc_log_replication_vnode_master) of
                    ok -> ok;
                    {missing, _Number} ->
                        % TODO: @gmcabrita, read 'OldBuffer' from log from Number till CurrentOp
                        % Send OldBuffer, after ok send Buffer
                        ok
                end
            end),
            ok
    end,
    LastRecord = lists:last(Buffer),
    LastOp = LastRecord#log_record.op_number#op_number.local,
    {reply, ok, State#state{last_ops = maps:put(OriginalPartition, LastOp, CurrentOps)}}.

open_log(Partition) ->
    LogFile = integer_to_list(Partition),
    LogId = LogFile ++ "--" ++ LogFile,
    LogPath = filename:join(app_helper:get_env(riak_core, platform_data_dir), LogId),
    case disk_log:open([{name, LogPath}]) of
        {ok, Log} -> Log;
        {repaired, Log, _, _} -> Log;
        {error, Reason} -> {error, Reason}
    end.

last_op_from_disk(Partition) ->
    case open_log(Partition) of
        {error, _Reason} -> 0;
        Log -> get_last_op_from_log(Log, start, 0)
    end.

get_last_op_from_log(Log, Continuation, MaxOp) ->
    ok = disk_log:sync(Log),
    case disk_log:chunk(Log, Continuation) of
        eof -> {eof, MaxOp};
        {error, Reason} -> {error, Reason};
        {NewContinuation, NewTerms} ->
            NewMaxOp = get_max_op_number(NewTerms, MaxOp),
            get_last_op_from_log(Log, NewContinuation, NewMaxOp);
        {NewContinuation, NewTerms, BadBytes} ->
            case BadBytes > 0 of
                true -> {error, bad_bytes};
                false ->
                    NewMaxOp = get_max_op_number(NewTerms, MaxOp),
                    get_last_op_from_log(Log, NewContinuation, NewMaxOp)
        end
    end.

get_max_op_number([], MaxOp) ->
    MaxOp;
get_max_op_number([{_LogId, #log_record{op_number = #op_number{local = N}}} | Rest], MaxOp) ->
    New = case N > MaxOp of
        true -> N;
        false -> MaxOp
    end,
    get_max_op_number(Rest, New).


filter_buffer(Buffer) ->
    lists:filter(fun(LogRecord) ->
        Operation = LogRecord#log_record.log_operation,
        case Operation#log_operation.op_type of
            update ->
                Update = Operation#log_operation.log_payload,
                Type = Update#update_log_payload.type,
                Op = Update#update_log_payload.op,
                not (antidote_ccrdt:is_type(Type) andalso Type:is_replicate_tagged(Op));
            _ -> true
        end
    end, Buffer).
