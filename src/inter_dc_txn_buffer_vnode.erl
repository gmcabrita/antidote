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

%% This vnode is responsible for collecting transactions for a small duration.
%% Once the timer runs out it passes the list of collected transactions to an
%% actor that attempts to compact CCRDT operations in those transactions.
%% The buffer of transactions is then wiped clean and the timer restarted.

-module(inter_dc_txn_buffer_vnode).
-behaviour(riak_core_vnode).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([
  buffer/2,
  compact/1]).

%% VNode Functions
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

%% Vnode State
-record(state, {
  partition :: partition_id(),
  buffer :: [#interdc_txn{}],
  timer :: any()
}).

%%%% API

%% Adds a transaction to the buffer.
%% Transactions in the buffer are compacted and broadcasted after the timer ends.
-spec buffer(partition_id(), #interdc_txn{}) -> ok.
buffer(Partition, Txn) -> dc_utilities:call_vnode(Partition, inter_dc_txn_buffer_vnode_master, {buffer, Txn}).

%%%% VNode Callbacks

start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
  {ok, set_timer(#state{
    partition = Partition,
    buffer = [],
    timer = none
  })}.

handle_command({buffer, Txn}, _Sender, State = #state{buffer = Buffer}) ->
  State1 = State#state{buffer = [Txn | Buffer]},
  {reply, ok, State1};
handle_command(send, _Sender, State = #state{buffer = Buffer}) ->
  case Buffer of
    [] -> ok;
    _ ->
      Buf = lists:reverse(Buffer),
      lager:info("Sending transactions from buffer: ~p~n", [Buf]),
      spawn(fun() ->
        compact_and_broadcast(Buf)
      end)
  end,
  State1 = set_timer(State#state{buffer = []}),
  {noreply, State1}.

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
terminate(_Reason, State) ->
  _ = del_timer(State),
  ok.

%%%% Functions

%% Cancels the send timer, if one is set.
-spec del_timer(#state{}) -> #state{}.
del_timer(State = #state{timer = none}) -> State;
del_timer(State = #state{timer = Timer}) ->
  _ = erlang:cancel_timer(Timer),
  State#state{timer = none}.

%% Sets the send timer.
-spec set_timer(#state{}) -> #state{}.
set_timer(State = #state{partition = Partition}) ->
  {ok, Ring} = riak_core_ring_manager:get_my_ring(),
  Node = riak_core_ring:index_owner(Ring, Partition),
  MyNode = node(),
  case Node of
    MyNode ->
      State1 = del_timer(State),
      Time = dc_utilities:now_millisec(),
      Timer = (Time + (?BUFFER_TXN_TIMER - (Time rem ?BUFFER_TXN_TIMER))) - Time,
      State1#state{timer = riak_core_vnode:send_command_after(Timer, send)};
    _Other -> State
  end.

%% Broadcasts a tuple of collection of transactions.
-spec broadcast_tuple({[#interdc_txn{}], [#interdc_txn{}]}) -> ok.
broadcast_tuple({BufferShort, BufferFull}) ->
  lists:foreach(fun(TxnTuple) -> inter_dc_pub:broadcast_tuple(TxnTuple) end, lists:zip(BufferShort, BufferFull)).

%% Compacts and broadcasts a collection of transactions.
-spec compact_and_broadcast([#interdc_txn{}]) -> ok.
compact_and_broadcast(Buffer) ->
  broadcast_tuple(compact(Buffer)).

%% Grabs the transaction id from an interdc_txn record.
get_txid(Txn) ->
  Record = hd(Txn#interdc_txn.log_records),
  Record#log_record.log_operation#log_operation.tx_id.

%% Compacts a collection of transactions.
%% If no computational CRDT operation is found in any of the transactions then
%% the output is the same as the input.
%%
%% Returns a tuple where the first field is the collection of transactions
%% with ALL operations, and the second field is the collection of transactions
%% without the computational CRDT replicate tagged operations.
-spec compact([#interdc_txn{}]) -> {[#interdc_txn{}], [#interdc_txn{}]}.
compact([]) -> [];
compact(Buffer) ->
  % TODO: @gmcabrita We currently use the transaction id of the last transaction
  % in the collection to build the final transaction. Perhaps we should generate
  % a new transaction ID instead.
  TxnId = get_txid(lists:last(Buffer)),
  % This builds:
  % - a map of {Key, Bucket} => [#log_record{}] for the computational CRDT ops;
  % - a list of #log_record{} for the non-computional CRDT ops;
  % - the list of transactions, where their log_records only contain prepare and commit ops.
  {CCRDTUpdateOps, ReversedOtherUpdateOps, ReversedTxns} =
    lists:foldl(fun(Txn = #interdc_txn{log_records = Logs}, {CCRDTOps, Ops, Txns}) ->
      {CCRDTUpdates, Updates, Other} = split_transaction_records(Logs, {CCRDTOps, Ops}, TxnId),
      CleanedTxn = Txn#interdc_txn{log_records = lists:reverse(Other)},
      {CCRDTUpdates, Updates, [CleanedTxn | Txns]}
    end, {#{}, [], []}, Buffer),

    % Compact the operation logs of the computational CRDTs.
    CompactedMapping = maps:map(fun(_, LogRecords) -> compact_log_records(lists:reverse(LogRecords)) end, CCRDTUpdateOps),
    % Get the transaction record we'll reuse to build the new one.
    Txn = hd(ReversedTxns),
    % Get the prev_log_opid from the first transaction in the buffer.
    FstTxn = hd(Buffer),
    PrevLogOpId = FstTxn#interdc_txn.prev_log_opid,
    % Get the tail log_records from the transaction we're reusing.
    Records = Txn#interdc_txn.log_records,
    % Build a list of all the compational CRDT compacted operations.
    OpsWithReplicate = lists:flatten(maps:values(CompactedMapping)),
    % Filter the replicate tagged operations out of the list.
    Ops = lists:filter(fun(X) ->
      {T, O} = get_type_and_op(X),
      not T:is_replicate_tagged(O)
    end, OpsWithReplicate),
    % Reverse to get the correct ordering.
    OtherUpdateOps = lists:reverse(ReversedOtherUpdateOps),
    CompactedTxnsWithReplicate = [Txn#interdc_txn{log_records = OtherUpdateOps ++ OpsWithReplicate ++ Records, prev_log_opid = PrevLogOpId}],
    CompactedTxns = [Txn#interdc_txn{log_records = OtherUpdateOps ++ Ops ++ Records, prev_log_opid = PrevLogOpId}],
    {CompactedTxns, CompactedTxnsWithReplicate}.

%% Splits a collection of transaction #log_record{} into a tuple containing:
%% - a map of {Key, Bucket} => [#log_record{}] for the computational CRDT ops;
%% - a list of #log_record{} for the non-computional CRDT ops;
%% - the list of #log_record{} for the prepare and commit ops.
-spec split_transaction_records([#log_record{}], {#{}, [#log_record{}]}, txid()) -> {#{}, [#log_record{}], #log_record{}}.
split_transaction_records(Logs, {CCRDTOps, Ops}, TxId) ->
  lists:foldl(fun(Log, Acc) -> place_txn_record(Log, Acc, TxId) end, {CCRDTOps, Ops, []}, Logs).

%% Updates the #log_record{} transaction id and places the #log_record{}
%% into the correct tuple slot.
-spec place_txn_record(#log_record{}, {#{}, [#log_record{}], [#log_record{}]}, txid()) -> {#{}, [#log_record{}], [#log_record{}]}.
place_txn_record(
  LogRecordArg = #log_record{
                log_operation = #log_operation{
                                  op_type = OpType,
                                  log_payload = LogPayload}}, {CCRDTOpsMap, UpdateOps, OtherOps}, TxId) ->
  % Update #log_record to the given TxId.
  LogOp = LogRecordArg#log_record.log_operation,
  LogRecord = LogRecordArg#log_record{log_operation = LogOp#log_operation{tx_id = TxId}},
  case OpType of
    update ->
      {Key, Bucket, Type, _Op} = destructure_update_payload(LogPayload),
      case antidote_ccrdt:is_type(Type) of
        true ->
          K = {Key, Bucket},
          NC = case maps:is_key(K, CCRDTOpsMap) of
            true ->
              Current = maps:get(K, CCRDTOpsMap),
              maps:put(K, [LogRecord | Current], CCRDTOpsMap);
            false -> maps:put(K, [LogRecord], CCRDTOpsMap)
          end,
          {NC, UpdateOps, OtherOps};
        false -> {CCRDTOpsMap, [LogRecord | UpdateOps], OtherOps}
      end;
    _ -> {CCRDTOpsMap, UpdateOps, [LogRecord | OtherOps]}
  end.

%% Gets the CRDT op() from a #log_record{}.
-spec get_op(#log_record{}) -> op().
get_op(#log_record{log_operation = #log_operation{log_payload = #update_log_payload{op = Op}}}) ->
  Op.

%% Gets the CRDT type() from a #log_record{}.
-spec get_type(#log_record{}) -> type().
get_type(#log_record{log_operation = #log_operation{log_payload = #update_log_payload{type = Type}}}) ->
  Type.

%% Gets the CRDT type() and op() from a #log_record{}.
get_type_and_op(#log_record{log_operation = #log_operation{log_payload = #update_log_payload{type = Type, op = Op}}}) ->
  {Type, Op}.

%% Replaces the CRDT op() in the nested record #log_record{} with the given op().
-spec replace_op(#log_record{}, op()) -> #log_record{}.
replace_op(LogRecord, Op) ->
  LogOp = LogRecord#log_record.log_operation,
  LogPayload = LogOp#log_operation.log_payload,
  LogRecord#log_record{
    log_operation = LogOp#log_operation{
      log_payload = LogPayload#update_log_payload{op = Op}
    }
  }.

%% Destructures an #update_log_payload{} record to the tuple {key(), bucket(), type(), op()}.
-spec destructure_update_payload(#update_log_payload{}) -> {key(), bucket(), type(), op()}.
destructure_update_payload(#update_log_payload{key = Key, bucket = Bucket, type = Type, op = Op}) ->
  {Key, Bucket, Type, Op}.

%% Compacts the given list of #log_record{} records.
%% It first builds a propagation log from the list of records, starting from
%% the earliest update operation.
%% Every time a record is added
-spec compact_log_records([#log_record{}]) -> [#log_record{}].
compact_log_records(LogRecords) ->
  % This builds a propagation log starting from scratch, adding each #log_record{} one-by-one.
  % Every time a new record is added using log/2 it attempts to compact the newly added record.
  lists:reverse(lists:foldl(fun(LogRecord, LogAcc) ->
    log(LogAcc, LogRecord)
  end, [], LogRecords)).

%% Adds a #log_record{} to the propagation log and does some compaction work.
-spec log([#log_record{}], #log_record{}) -> [#log_record{}].
log(LogAcc, LogRecord) ->
  case log_(LogAcc, LogRecord) of
    {ok, Logs} -> Logs;
    {rerun, Logs, NewLogRecord} -> log(Logs, NewLogRecord);
    {err, Logs} -> [LogRecord | Logs]
  end.

%% Helper function for log/2.
-spec log_([#log_record{}], #log_record{}) -> {ok | err, [#log_record{}]}.
log_([], _) -> {err, []};
log_([LogRecord2 | Rest], LogRecord1) ->
  Type = get_type(LogRecord1),
  Op1 = get_op(LogRecord1),
  Op2 = get_op(LogRecord2),
  case Type:can_compact(Op2, Op1) of
    true ->
      case Type:compact_ops(Op2, Op1) of
        {noop} -> {ok, Rest};
        NewOp ->
          NewRecord = replace_op(LogRecord1, NewOp),
          % Compaction was possible, but we should keep going back in the log,
          % since it may be possible to compact more operations.
          case log_(Rest, NewRecord) of
            {ok, List} -> {ok, List};
            {err, List} -> {ok, [NewRecord | List]}
          end
      end;
    false ->
      % Could not compact the two operations, but we can still commmute them.
      case log_(Rest, LogRecord1) of
        {ok, List} -> {ok, [LogRecord2 | List]};
        {err, _} -> {err, [LogRecord2 | Rest]}
      end
  end.

%%% Tests

-ifdef(TEST).

compare_txn_sets({Short1, Full1} , {Short2, Full2}) ->
  compare_txns(Short1, Short2),
  compare_txns(Full1, Full2).

compare_txns([Tx1], [Tx2]) ->
  ?assertEqual(Tx1#interdc_txn.dcid, Tx2#interdc_txn.dcid),
  ?assertEqual(Tx1#interdc_txn.partition, Tx2#interdc_txn.partition),
  ?assertEqual(Tx1#interdc_txn.prev_log_opid, Tx2#interdc_txn.prev_log_opid),
  ?assertEqual(Tx1#interdc_txn.snapshot, Tx2#interdc_txn.snapshot),
  ?assertEqual(Tx1#interdc_txn.timestamp, Tx2#interdc_txn.timestamp),
  Set1 = sets:from_list(Tx1#interdc_txn.log_records),
  Set2 = sets:from_list(Tx2#interdc_txn.log_records),
  ?assertEqual(sets:is_subset(Set1, Set2), true),
  ?assertEqual(sets:is_subset(Set2, Set1), true).

inter_dc_txn_from_ops(Ops, PrevLogOpId, N, TxId, CommitTime, SnapshotTime) ->
  {Records, Number} = lists:foldl(fun({Key, Bucket, Type, Op}, {List, Number}) ->
    Record = #log_record{
      version = 0,
      op_number = Number,
      bucket_op_number = Number,
      log_operation = #log_operation{
        tx_id = TxId,
        op_type = update,
        log_payload = #update_log_payload{
          key = Key,
          bucket = Bucket,
          type = Type,
          op = Op
        }
      }
    },
    {[Record | List], Number + 1}
  end, {[], N}, Ops),
  {RecordsCCRDT, RecordsOther, _} = split_transaction_records(lists:reverse(Records), {#{}, []}, TxId),
  Prepare = #log_record{version = 0, op_number = Number, bucket_op_number = Number, log_operation = #log_operation{tx_id = TxId, op_type = prepare, log_payload = #prepare_log_payload{prepare_time = CommitTime - 1}}},
  Commit = #log_record{version = 0, op_number = Number + 1, bucket_op_number = Number + 1, log_operation = #log_operation{tx_id = TxId, op_type = commit, log_payload = #commit_log_payload{commit_time = CommitTime, snapshot_time = SnapshotTime}}},
  LogRecords = lists:reverse(RecordsOther) ++ lists:flatten(lists:map(fun lists:reverse/1, maps:values(RecordsCCRDT))) ++ [Prepare, Commit],
  #interdc_txn{
    dcid = replica1,
    partition = 1,
    prev_log_opid = PrevLogOpId,
    snapshot = SnapshotTime,
    timestamp = CommitTime,
    log_records = LogRecords
  }.

empty_txns_test() ->
  ?assertEqual(compact([]), []).

no_ccrdts_test() ->
  Buffer1 = [
    inter_dc_txn_from_ops([{key, bucket, non_ccrdt, some_operation}],
                          0,
                          1,
                          1,
                          200,
                          50)
  ],
  ?assertEqual(compact(Buffer1), {Buffer1, Buffer1}),
  Buffer2 = Buffer1 ++ [inter_dc_txn_from_ops([{key, bucket, non_ccrdt, some_operation}], 1, 2, 2, 300, 250)],
  Expected = [
    inter_dc_txn_from_ops([{key, bucket, non_ccrdt, some_operation},
                           {key, bucket, non_ccrdt, some_operation}],
                          0,
                          1,
                          2,
                          300,
                          250)
  ],
  ?assertEqual(compact(Buffer2), {Expected, Expected}).

replicate_ops_test() ->
  Type = antidote_ccrdt_topk_with_deletes,
  Buffer = [
    inter_dc_txn_from_ops([{a, b, Type, {add, {0, 5, {foo, 1}}}},
                           {a, b, Type, {replicate_add, {0, 5, {foo, 1}}}},
                           {a, b, Type, {replicate_add, {0, 40, {foo, 2}}}},
                           {a, b, Type, {replicate_add, {0, 50, {foo, 3}}}},
                           {a, b, Type, {replicate_add, {0, 51, {foo, 4}}}},
                           {a, b, Type, {replicate_del, {0, #{foo => {foo, 3}}}}},
                           {a, b, Type, {replicate_add, {0, 100, {foo, 5}}}},
                           {a, b, Type, {del, {0, #{foo => {foo, 4}}}}}],
                          0,
                          1,
                          1,
                          200,
                          50)
  ],
  Expected = [
    inter_dc_txn_from_ops([{a, b, Type, {replicate_add, {0, 100, {foo, 5}}}},
                           {a, b, Type, {del, {0, #{foo => {foo, 4}}}}}],
                          0,
                          7,
                          1,
                          200,
                          50)
  ],
  ExpectedWithoutReplicate = [
    inter_dc_txn_from_ops([{a, b, Type, {del, {0, #{foo => {foo, 4}}}}}],
                          0,
                          8,
                          1,
                          200,
                          50)
  ],
  compare_txn_sets(compact(Buffer), {ExpectedWithoutReplicate, Expected}).

different_ccrdt_types_test() ->
  TopkD = antidote_ccrdt_topk_with_deletes,
  Topk = antidote_ccrdt_topk,
  Average = antidote_ccrdt_average,
  Buffer = [
    inter_dc_txn_from_ops([{topkd, bucket, TopkD, {add, {0, 5, {foo, 1}}}},
                           {topk, bucket, Topk, {add, {100, 5}}},
                           {average, bucket, Average, {add, {10, 1}}},
                           {topkd, bucket, TopkD, {del, {0, #{foo => {foo, 1}}}}},
                           {topk, bucket, Topk, {add, {100, 42}}},
                           {average, bucket, Average, {add, {100, 2}}}],
                          0,
                          1,
                          1,
                          200,
                          50)
  ],
  Expected = [
    inter_dc_txn_from_ops([{topkd, bucket, TopkD, {del, {0, #{foo => {foo, 1}}}}},
                           {topk, bucket, Topk, {add, {100, 42}}},
                           {average, bucket, Average, {add, {110, 3}}}],
                          0,
                          4,
                          1,
                          200,
                          50)
  ],
  ?assertEqual(compact(Buffer), {Expected, Expected}).

txn_ccrdt_mixed_with_crdt_test() ->
  CCRDT = antidote_ccrdt_topk_with_deletes,
  Buffer = [
    inter_dc_txn_from_ops([{top, bucket, CCRDT, {add, {0, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {del, {0, #{foo => {foo, 1}}}}}],
                          0,
                          1,
                          1,
                          100,
                          50),
    inter_dc_txn_from_ops([{a, bucket, not_a_ccrdt, {add, {100, 5, {foo, 1}}}},
                           {a, bucket, not_a_ccrdt, {add, {77, 5, {foo, 1}}}}],
                          2,
                          3,
                          2,
                          200,
                          150)
  ],
  Expected = [
    inter_dc_txn_from_ops([{top, bucket, CCRDT, {del, {0, #{foo => {foo, 1}}}}},
                           {a, bucket, not_a_ccrdt, {add, {100, 5, {foo, 1}}}},
                           {a, bucket, not_a_ccrdt, {add, {77, 5, {foo, 1}}}}],
                          0,
                          2,
                          2,
                          200,
                          150)
  ],
  ?assertEqual(compact(Buffer), {Expected, Expected}).

compactable_txn_test() ->
  CCRDT = antidote_ccrdt_topk_with_deletes,
  Buffer = [
    inter_dc_txn_from_ops([{top, bucket, CCRDT, {add, {0, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {del, {0, #{foo => {foo, 1}}}}}],
                          0,
                          1,
                          1,
                          150,
                          200)
  ],
  Expected = [
    inter_dc_txn_from_ops([{top, bucket, CCRDT, {del, {0, #{foo => {foo, 1}}}}}],
                          0,
                          2,
                          1,
                          150,
                          200)
  ],
  ?assertEqual(compact(Buffer), {Expected, Expected}).

two_ccrdt_txn_not_compactable_test() ->
  CCRDT = antidote_ccrdt_topk_with_deletes,
  Buffer = [
    inter_dc_txn_from_ops([{top, bucket, CCRDT, {add, {0, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {1, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {2, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {3, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {4, 5, {foo, 1}}}}],
                          0,
                          1,
                          1,
                          100,
                          50),
    inter_dc_txn_from_ops([{top, bucket, CCRDT, {add, {5, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {6, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {7, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {8, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {9, 5, {foo, 1}}}}],
                          5,
                          6,
                          2,
                          200,
                          150)
  ],
  Expected = [
    inter_dc_txn_from_ops([{top, bucket, CCRDT, {add, {0, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {1, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {2, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {3, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {4, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {5, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {6, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {7, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {8, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {9, 5, {foo, 1}}}}],
                          0,
                          1,
                          2,
                          200,
                          150)
  ],
  ?assertEqual(compact(Buffer), {Expected, Expected}).

single_ccrdt_txn_not_compactable_test() ->
  CCRDT = antidote_ccrdt_topk_with_deletes,
  Buffer = [
    inter_dc_txn_from_ops([{top, bucket, CCRDT, {add, {0, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {1, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {2, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {3, 5, {foo, 1}}}},
                           {top, bucket, CCRDT, {add, {4, 5, {foo, 1}}}}],
                          0,
                          1,
                          1,
                          100,
                          50)
  ],
  ?assertEqual(compact(Buffer), {Buffer, Buffer}).

-endif.