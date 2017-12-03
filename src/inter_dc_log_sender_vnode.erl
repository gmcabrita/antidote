%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
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

%% Each logging_vnode informs this vnode about every new appended operation.
%% This vnode assembles operations into transactions, and sends the transactions to appropriate destinations.
%% If no transaction is sent in 10 seconds, heartbeat messages are sent instead.

-module(inter_dc_log_sender_vnode).
-behaviour(riak_core_vnode).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").


%% API
-export([
    send/2,
    get_buffer/2,
    update_last_log_id/2,
    start_timer/1,
    send_stable_time/2]).

%% VNode methods
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

%% Vnode state
-record(state, {
    partition :: partition_id(),
    buffer, %% log_tx_assembler:state
    last_log_id :: #op_number{}, % last log id that was broadcasted
    timer :: any(), % when this timer ends a heartbeat is propagated
    %% The record fields below are used for collecting transactions for a small duration of time.
    %% Once the buffer timer runs out it compacts the collection of transactions and
    %% then broadcasts it to remote DCs.
    %% The txn_buffer is then wiped clean and the buffer_timer restarted.
    %% Note: these are only used if ?BUFFER_TXNS is true.
    txn_buffer :: [#interdc_txn{}], % collection of completed transactions
    buffer_timer :: any() % when this timer
}).

%%%% API --------------------------------------------------------------------+

%% Send the new operation to the log_sender.
%% The transaction will be buffered until all the operations in a transaction are collected,
%% and then the transaction will be broadcasted via interDC.
%% WARNING: only LOCALLY COMMITED operations (not from remote DCs) should be sent to log_sender_vnode.
-spec send(partition_id(), #log_record{}) -> ok.
send(Partition, LogRecord) -> dc_utilities:call_vnode(Partition, inter_dc_log_sender_vnode_master, {log_event, LogRecord}).

get_buffer(Partition, TxId) -> dc_utilities:call_vnode_sync(Partition, inter_dc_log_sender_vnode_master, {get_buffer, TxId}).

%% Start the heartbeat timer
-spec start_timer(partition_id()) -> ok.
start_timer(Partition) -> dc_utilities:call_vnode_sync(Partition, inter_dc_log_sender_vnode_master, {start_timer}).

%% After restarting from failure, load the operation id of the last operation sent by this DC
%% Otherwise the stable time won't advance as the receving DC will be thinking it is getting old messages
-spec update_last_log_id(partition_id(), #op_number{}) -> ok.
update_last_log_id(Partition, OpId) -> dc_utilities:call_vnode_sync(Partition, inter_dc_log_sender_vnode_master, {update_last_log_id, OpId}).

%% Send the stable time to this vnode, no transaction in the future will commit with a smaller time
-spec send_stable_time(partition_id(), non_neg_integer()) -> ok.
send_stable_time(Partition, Time) ->
    dc_utilities:call_local_vnode(Partition, inter_dc_log_sender_vnode_master, {stable_time, Time}).

%%%% VNode methods ----------------------------------------------------------+

start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, set_buffer_timer(#state{
        partition = Partition,
        buffer = log_txn_assembler:new_state(),
        last_log_id = #op_number{},
        timer = none,
        txn_buffer = [],
        buffer_timer = none
    })}.

%% Start the timer
handle_command({start_timer}, _Sender, State) ->
    {reply, ok, set_heartbeat_timer(true, State)};

handle_command({update_last_log_id, OpId}, _Sender, State = #state{partition = Partition}) ->
    lager:debug("Updating last log id at partition ~w to: ~w", [Partition, OpId]),
    {reply, ok, State#state{last_log_id = OpId}};

handle_command({get_buffer, TxId}, _Sender, State) ->
    Log = log_txn_assembler:find_or_default(TxId, [], State#state.buffer),
    {reply, Log, State};

%% Handle the new operation
%% -spec handle_command({log_event, #log_record{}}, pid(), #state{}) -> {noreply, #state{}}.
handle_command({log_event, LogRecord}, _Sender, State) ->
    %% Use the txn_assembler to check if the complete transaction was collected.
    {Result, NewBufState} = log_txn_assembler:process(LogRecord, State#state.buffer),
    State1 = State#state{buffer = NewBufState},
    State2 = case Result of
        %% If the transaction was collected
        {ok, Ops} ->
            Txn = inter_dc_txn:from_ops(Ops, State1#state.partition, State#state.last_log_id),
            case ?BUFFER_TXNS of
                true -> buffer(State1, Txn);
                false -> broadcast(State1, Txn)
            end;
        %% If the transaction is not yet complete
        none -> State1
    end,
    {noreply, State2};

handle_command(txn_send, _Sender, State = #state{txn_buffer = Buffer}) ->
    FinalState = case Buffer of
        [] -> State;
        _ ->
            Buf = lists:reverse(Buffer),
            spawn(fun() ->inter_dc_txn_buffer:compact_and_broadcast(Buf) end),
            OpId = inter_dc_txn:last_log_opid(hd(Buffer)),
            % Reset heartbeat timer
            set_heartbeat_timer(State#state{txn_buffer = [], last_log_id = OpId})
    end,
    {noreply, set_buffer_timer(FinalState)};

handle_command({stable_time, Time}, _Sender, State) ->
    PingTxn = inter_dc_txn:ping(State#state.partition, State#state.last_log_id, Time),
    {noreply, set_heartbeat_timer(broadcast(State, PingTxn))};

handle_command({hello}, _Sender, State) ->
    {reply, ok, State};

%% Handle the ping request, managed by the timer (1s by default)
handle_command(ping, _Sender, State) ->
    get_stable_time(State#state.partition),
    {noreply, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.
handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.
handoff_starting(_TargetNode, State) ->
    {true, State}.
handoff_cancelled(State) ->
     S1 = set_heartbeat_timer(State),
    {ok, set_buffer_timer(S1)}.
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
terminate(_Reason, State) ->
    _ = del_heartbeat_timer(State),
    _ = del_buffer_timer(State),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%

%% Cancels the ping timer, if one is set.
-spec del_heartbeat_timer(#state{}) -> #state{}.
del_heartbeat_timer(State = #state{timer = none}) -> State;
del_heartbeat_timer(State = #state{timer = Timer}) ->
    _ = erlang:cancel_timer(Timer),
    State#state{timer = none}.

%% Cancels the previous ping timer and sets a new one.
-spec set_heartbeat_timer(#state{}) -> #state{}.
set_heartbeat_timer(State) ->
    set_heartbeat_timer(false,State).

-spec set_heartbeat_timer(boolean(), #state{}) -> #state{}.
set_heartbeat_timer(First, State = #state{partition = Partition}) ->
    case First of
        true ->
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            Node = riak_core_ring:index_owner(Ring, Partition),
            MyNode = node(),
            case Node of
                MyNode ->
                    State1 = del_heartbeat_timer(State),
                    State1#state{timer = riak_core_vnode:send_command_after(?HEARTBEAT_PERIOD, ping)};
                _Other ->
                    State
            end;
        false ->
            State1 = del_heartbeat_timer(State),
            State1#state{timer = riak_core_vnode:send_command_after(?HEARTBEAT_PERIOD, ping)}
    end.

%% Cancels the txn buffer timer, if one is set.
-spec del_buffer_timer(#state{}) -> #state{}.
del_buffer_timer(State = #state{buffer_timer = none}) -> State;
del_buffer_timer(State = #state{buffer_timer = Timer}) ->
    _ = erlang:cancel_timer(Timer),
    State#state{buffer_timer = none}.

%% Cancels the previous txn buffer timer and sets a new one.
-spec set_buffer_timer(#state{}) -> #state{}.
set_buffer_timer(State) ->
    case ?BUFFER_TXNS of
        true -> set_buffer_timer(false,State);
        false -> State
    end.

-spec set_buffer_timer(boolean(), #state{}) -> #state{}.
set_buffer_timer(First, State = #state{partition = Partition}) ->
    case First of
        true ->
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            Node = riak_core_ring:index_owner(Ring, Partition),
            MyNode = node(),
            case Node of
                MyNode ->
                    State1 = del_buffer_timer(State),
                    State1#state{buffer_timer = riak_core_vnode:send_command_after(?BUFFER_TXN_TIMER, txn_send)};
                _Other ->
                    State
            end;
        false ->
            State1 = del_buffer_timer(State),
            State1#state{buffer_timer = riak_core_vnode:send_command_after(?BUFFER_TXN_TIMER, txn_send)}
    end.

%% Broadcasts the transaction via local publisher.
-spec broadcast(#state{}, #interdc_txn{}) -> #state{}.
broadcast(State, Txn) ->
  inter_dc_pub:broadcast(Txn),
  Id = inter_dc_txn:last_log_opid(Txn),
  State#state{last_log_id = Id}.

%% Buffers the transaction so its operations can be compacted with operations in
% other buffered transactions.
-spec buffer(#state{}, #interdc_txn{}) -> #state{}.
buffer(#state{txn_buffer = Buffer} = State, Txn) ->
    State#state{txn_buffer = [Txn | Buffer]}.

%% @doc Sends an async request to get the smallest snapshot time of active transactions.
%%      No new updates with smaller timestamp will occur in future.
-spec get_stable_time(partition_id()) -> ok.
get_stable_time(Partition) ->
    ok = clocksi_vnode:send_min_prepared(Partition).
