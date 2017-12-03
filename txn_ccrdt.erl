#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name txn@127.0.0.1 -cookie antidote

-mode(compile).

main([Target]) ->
    erlang:set_cookie(node(), antidote),
    O = {obj_ccrdt, antidote_ccrdt_topk_rmv, bucket},
    L = lists:seq(1, 102),
    T = list_to_atom(Target),
    lists:map(fun(N) ->
        rpc:call(T, antidote, update_objects, [ignore, [], [{O, add, {N, N}}]])
    end, L),
    rpc:call(T, antidote, update_objects, [ignore, [], [{O, rmv, 102}]]),
    lists:map(fun(N) ->
        rpc:call(T, antidote, update_objects, [ignore, [], [{O, add, {N, N}}]])
    end, lists:seq(-100, -51)).
