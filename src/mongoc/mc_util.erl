%%%-------------------------------------------------------------------
%%% @author mononym
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%% helper functions accessed from various modules
%%% @end
%%%-------------------------------------------------------------------
-module(mc_util).

-include("mongoc.hrl").

%% API
-export([form_connect_args/4, parse_seed/1, wait_connect_complete/2]).


%%%===================================================================
%%% API
%%%===================================================================
form_connect_args(Host, Port, Timeout, WorkerArgs) ->
  [{host, Host}, {port, Port}, {timeout, Timeout} | WorkerArgs].

parse_seed(Addr) when is_binary(Addr) ->
  parse_seed(binary_to_list(Addr));
parse_seed(Addr) when is_list(Addr) ->
  [Host0, Port] = string:split(Addr, ":", trailing),
  Host = case inet:parse_address(Host0) of
             {ok, H} -> H;
             _ -> Host0
         end,
  {Host, list_to_integer(Port)}.

wait_connect_complete(PoolSize, Timeout) when PoolSize > 0 ->
    try
        Responses = collect_worker_responses(PoolSize, Timeout),
        case lists:filter(fun(R) -> R =/= connect_complete end, Responses) of
            [] -> ok;
            [{error, FirstError} | _] -> {error, FirstError}
        end
    catch
        throw:timeout ->
            logger:log(error, "[ecpool_worker_sup] wait_connect_complete timeout"),
            {error, timeout}
    end.

collect_worker_responses(PoolSize, Timeout) ->
    [
        receive {mc_worker_reply, Resp} -> Resp
        after Timeout -> %% the overall timeout waiting for all workers
            throw(timeout)
        end || _ <- lists:seq(1, PoolSize)
    ].
