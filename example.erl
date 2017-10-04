#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name metrics@127.0.0.1 -cookie antidote

-mode(compile).

main([Target]) ->
    erlang:set_cookie(node(), antidote),
    R = rpc:call(list_to_atom(Target), antidote_stats, get_value, [[divergence]]),
    io:format("~p~n", [R]).
