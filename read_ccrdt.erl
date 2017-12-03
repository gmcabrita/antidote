#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name tnx@127.0.0.1 -cookie antidote

-mode(compile).

main([Target]) ->
    erlang:set_cookie(node(), antidote),
    O = {obj_ccrdt, antidote_ccrdt_topk_rmv, bucket},
    R = rpc:call(list_to_atom(Target), antidote, read_objects, [ignore, [], [O]]),
    io:format("~p~n", [R]).
