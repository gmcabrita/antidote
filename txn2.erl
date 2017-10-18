#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name txn@127.0.0.1 -cookie antidote

-mode(compile).

main([Target]) ->
    erlang:set_cookie(node(), antidote),
    O = {a_set, antidote_crdt_gset, bucket},
    E = rand:uniform(100000),
    R = rpc:call(list_to_atom(Target), antidote, update_objects, [ignore, [], [{O, add, E}]]),
    io:format("~p~n", [R]).
