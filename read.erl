#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name txn@127.0.0.1 -cookie antidote

-mode(compile).

main([Target]) ->
    erlang:set_cookie(node(), antidote),
    O = {obj, antidote_crdt_counter, bucket},
    R = rpc:call(list_to_atom(Target), antidote, read_objects, [ignore, [], [O]]),
    io:format("~p~n", [R]).
