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
-export([form_connect_args/4, parse_seed/1]).


%%%===================================================================
%%% API
%%%===================================================================

form_connect_args(Host, Port, Timeout, WorkerArgs) ->
  case mc_utils:get_value(ssl, WorkerArgs, false) of
    true -> [{host, Host}, {port, Port}, {timeout, Timeout}, {ssl, true},
      {ssl_opts, mc_utils:get_value(ssl_opts, WorkerArgs, [])}];
    false -> [{host, Host}, {port, Port}, {timeout, Timeout}]
  end.

parse_seed(Addr) when is_binary(Addr) ->
  parse_seed(binary_to_list(Addr));
parse_seed(Addr) when is_list(Addr) ->
  [Host0, Port] = string:split(Addr, ":", trailing),
  Host = case inet:parse_address(Host0) of
             {ok, H} -> H;
             _ -> Host0
         end,
  {Host, list_to_integer(Port)}.
