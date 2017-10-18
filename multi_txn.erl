#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name metrics@127.0.0.1 -cookie antidote

-mode(compile).

% main([Target]) ->
%     erlang:set_cookie(node(), antidote),
%     O = {obj, antidote_crdt_counter, bucket},
%     case rpc:call(list_to_atom(Target), antidote, update_objects, [ignore, [], [{O, increment, 1}]]) of
%         {ok, R} -> io:format("~p~n", [R]);
%         A -> io:format("Error: ~p~n", [A])
%     end.

main([Target]) ->
    erlang:set_cookie(node(), antidote),
    O1 = {obj1, antidote_crdt_counter, bucket},
    O2 = {obj2, antidote_crdt_counter, bucket},
    write(Target, O1),
    write(Target, O2),
    read(Target, [O1]),
    read(Target, [O2]),
    read(Target, [O1, O2]).

read(Target, Objects) ->
    io:format("~p~n", [rpc:call(list_to_atom(Target), antidote, read_objects, [ignore, [], Objects])]).

write(Target, O) ->
    {ok, TxId} = rpc:call(list_to_atom(Target), antidote, start_transaction, [ignore, []]),

    ok = rpc:call(list_to_atom(Target), antidote, update_objects, [[{O, increment, 1}], TxId]),
    ok = rpc:call(list_to_atom(Target), antidote, update_objects, [[{O, increment, 5}], TxId]),
    ok = rpc:call(list_to_atom(Target), antidote, update_objects, [[{O, increment, 7}], TxId]),
    ok = rpc:call(list_to_atom(Target), antidote, update_objects, [[{O, increment, 8}], TxId]),
    {ok, CT} = rpc:call(list_to_atom(Target), antidote, commit_transaction, [TxId]),
    io:format("~p~n", [CT]).
