%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Jul 2016 15:15
%%%-------------------------------------------------------------------
-module(mc_topology_logics).
-author("tihon").

-include("mongoc.hrl").

-define(NON_SHARDED(ST),
  (ST =:= standalone orelse ST =:= rsPrimary orelse ST =:= rsSecondary orelse ST =:= rsArbiter orelse ST =:= rsOther orelse ST =:= rsGhost)).
-define(SEC_ARB_OTH(ST), (ST =:= rsSecondary orelse ST =:= rsArbiter orelse ST =:= rsOther)).
-define(STAL_MONGS(ST), (ST =:= standalone orelse ST =:= mongos)).
-define(UNKN_GHST(ST), (ST =:= unknown orelse ST =:= rsGhost)).
-define(NOT_MAX(E, M), (E =/= undefined andalso M =/= undefined andalso E < M)).

-define(LOG_TOPOLOGY_ERROR(Configured, Actual),
  logger:error("Configured mongo client topology does not match actual mongo install topology. Configured: ~p; Actual: ~p", [Configured, Actual])).

-define(LOG_SET_NAME_ERROR(Configured, Actual),
  logger:error("Configured mongo set name does not match actual mongo install set name. Configured: ~p; Actual: ~p", [Configured, Actual])).

%% API
-export([update_topology_state/2, init_seeds/4, init_seeds/1, validate_server_and_config/3, server_type/1]).

update_topology_state(#mc_server{type = SType, pid = Pid}, State = #topology_state{type = sharded}) when ?NON_SHARDED(SType) -> %% SHARDED
  ?LOG_TOPOLOGY_ERROR(sharded, SType),
  exit(Pid, kill),
  State;
update_topology_state(_, State = #topology_state{type = sharded}) ->
  State;
update_topology_state(#mc_server{type = unknown}, State = #topology_state{type = unknown}) -> %% UNKNOWN
  State;
update_topology_state(#mc_server{type = rsGhost}, State = #topology_state{type = unknown}) ->
  State;
update_topology_state(#mc_server{type = standalone}, State = #topology_state{type = unknown, seeds = Seeds}) when length(Seeds) =< 1 ->
  State#topology_state{type = standalone};
update_topology_state(#mc_server{type = standalone, pid = Pid}, State = #topology_state{type = unknown}) ->
  ?LOG_TOPOLOGY_ERROR(unknown, standalone),
  exit(Pid, kill),
  State;
update_topology_state(#mc_server{type = mongos}, State = #topology_state{type = unknown}) ->
  State#topology_state{type = sharded};
update_topology_state(Server = #mc_server{type = SType, setName = SetName},
    State = #topology_state{type = unknown, setName = SetName}) when ?SEC_ARB_OTH(SType) ->
  update_topology_state(Server, State#topology_state{setName = undefined});
update_topology_state(
    #mc_server{type = SType, setName = SetName, hosts = Hosts, arbiters = Arbiters, passives = Passives, primary = Primary},
    State = #topology_state{type = unknown, setName = undefined, topology_opts = Topts, worker_opts = Wopts, servers = Tab})
  when ?SEC_ARB_OTH(SType) ->
  init_seeds(lists:flatten([Hosts, Arbiters, Passives]), Tab, Topts, Wopts),
  set_possible_primary(Tab, Primary),
  State#topology_state{type = checkIfHasPrimary(Tab), setName = SetName};
update_topology_state(#mc_server{type = SType, pid = Pid}, State = #topology_state{type = unknown}) when ?SEC_ARB_OTH(SType) ->
  ?LOG_TOPOLOGY_ERROR(unknown, SType),
  exit(Pid, kill),
  State;
update_topology_state(#mc_server{type = SType, pid = Pid},
    State = #topology_state{type = replicaSetNoPrimary}) when ?STAL_MONGS(SType) ->  %% REPLICASETNOPRIMARY
  ?LOG_TOPOLOGY_ERROR(replicaSetNoPrimary, SType),
  exit(Pid, kill),
  State;
update_topology_state(Server = #mc_server{type = SType, setName = SetName},
    State = #topology_state{type = replicaSetNoPrimary, setName = SetName}) when ?SEC_ARB_OTH(SType) ->
  update_topology_state(Server, State#topology_state{setName = undefined});
update_topology_state(
    #mc_server{type = SType, setName = SetName, hosts = Hosts, arbiters = Arbiters, passives = Passives, primary = Primary},
    State = #topology_state{type = replicaSetNoPrimary, setName = undefined, topology_opts = Topts, worker_opts = Wopts, servers = Tab})
  when ?SEC_ARB_OTH(SType) ->
  init_seeds(lists:flatten([Hosts, Arbiters, Passives]), Tab, Topts, Wopts),
  set_possible_primary(Tab, Primary),
  State#topology_state{setName = SetName};
update_topology_state(#mc_server{type = SType, pid = Pid, setName = SSetName}, State = #topology_state{type = replicaSetNoPrimary, setName = TSetName}) when ?SEC_ARB_OTH(SType) ->
  ?LOG_SET_NAME_ERROR(TSetName, SSetName),
  exit(Pid, kill),
  State;
update_topology_state(#mc_server{type = SType},
    State = #topology_state{type = replicaSetWithPrimary, servers = Tab}) when ?UNKN_GHST(SType) -> %% REPLICASETWITHPRIMARY
  State#topology_state{type = checkIfHasPrimary(Tab)};
update_topology_state(#mc_server{type = SType, pid = Pid},
    State = #topology_state{type = replicaSetWithPrimary, servers = Tab}) when ?STAL_MONGS(SType) ->
  ?LOG_TOPOLOGY_ERROR(replicaSetWithPrimary, SType),
  exit(Pid, kill),
  State#topology_state{type = checkIfHasPrimary(Tab)};
update_topology_state(
    #mc_server{type = SType, setName = SetName, primary = Primary},
    State = #topology_state{type = replicaSetWithPrimary, setName = SetName, servers = Tab}) when ?SEC_ARB_OTH(SType) ->
  set_possible_primary(Tab, Primary),
  State#topology_state{type = checkIfHasPrimary(Tab)};
update_topology_state(
    #mc_server{type = SType, pid = Pid, setName = SSetName},
    State = #topology_state{type = replicaSetWithPrimary, servers = Tab, setName = TSetName}) when ?SEC_ARB_OTH(SType) ->
  ?LOG_SET_NAME_ERROR(TSetName, SSetName),
  exit(Pid, kill),
  State#topology_state{type = checkIfHasPrimary(Tab)};
update_topology_state(Server = #mc_server{type = rsPrimary, setName = SetName}, State = #topology_state{setName = SetName}) -> %% REPLICASETWITHPRIMARY
  update_topology_state(Server, State#topology_state{setName = undefined});
update_topology_state(#mc_server{type = rsPrimary, electionId = ElectionId, host = Host, pid = Pid},
    State = #topology_state{setName = undefined, maxElectionId = MaxElectionId, servers = Tab}) when ?NOT_MAX(ElectionId, MaxElectionId) ->
  ets:insert(Tab, #mc_server{pid = Pid, host = Host, type = unknown}),
  State#topology_state{type = checkIfHasPrimary(Tab)};
update_topology_state(
    #mc_server{type = rsPrimary, setName = SetName, electionId = ElectionId, hosts = Hosts, arbiters = Arbiters, passives = Passives},
    State = #topology_state{setName = undefined, topology_opts = Topts, worker_opts = Wopts, servers = Tab}) ->
  HostsList = lists:flatten([Hosts, Arbiters, Passives]),
  init_seeds(HostsList, Tab, Topts, Wopts),
  %stop_servers_not_in_list(HostsList, Tab),
  State#topology_state{setName = SetName, maxElectionId = ElectionId, type = checkIfHasPrimary(Tab)};
update_topology_state(#mc_server{type = rsPrimary, pid = Pid, host = Host, setName = SSetName},
    State = #topology_state{setName = CSetName, servers = Tab}) when SSetName =/= CSetName ->
  ets:insert(Tab, #mc_server{pid = Pid, host = Host, type = deleted}),
  ?LOG_SET_NAME_ERROR(CSetName, SSetName),
  exit(Pid, kill),
  State#topology_state{type = checkIfHasPrimary(Tab)};
update_topology_state(_, State) ->
  State.

init_seeds(#topology_state{seeds = Seeds, topology_opts = Topts, worker_opts = Wopts, servers = Tab}) ->
  init_seeds(Seeds, Tab, Topts, Wopts).

init_seeds([], _, _, _) -> ok;
init_seeds([Addr | Seeds], Tab, Topts, Wopts) ->
  Host = mc_utils:to_binary(Addr),
  Saved = ets:select(Tab, [{#mc_server{host = Host, _ = '_'}, [], ['$_']}]),
  start_seed(Saved, Host, Tab, Topts, Wopts),
  init_seeds(Seeds, Tab, Topts, Wopts).

validate_server_and_config(ConnectArgs, TopologyType, TopologySetName) ->
  case mc_worker_api:connect(ConnectArgs) of
    {ok, Conn} ->
      {true, MaybeMaster} =
          case mc_utils:use_legacy_protocol(Conn) of
              true ->
                  mc_worker_api:command(Conn, {isMaster, 1});
              false ->
                  mc_worker_api:command(Conn, {hello, 1})
          end,
      mc_worker_api:disconnect(Conn),
      ServerType = server_type(MaybeMaster),
      ServerSetName = maps:get(<<"setName">>, MaybeMaster, undefined),
      validate_server_and_config(TopologyType, TopologySetName, ServerType, ServerSetName);
    {error, Reason} ->
      {connect_failed, Reason}
  end.

validate_server_and_config(TopologyType, TopologySetName, ServerType, ServerSetName) ->
  case TopologyType of
    unknown when ?SEC_ARB_OTH(ServerType) ->
      ?LOG_TOPOLOGY_ERROR(unknown, ServerType),
      {configured_mongo_type_mismatch, TopologyType, ServerType};

    unknown when ServerType == standalone ->
      ?LOG_TOPOLOGY_ERROR(unknown, ServerType),
      {configured_mongo_type_mismatch, TopologyType, ServerType};

    sharded when ?NON_SHARDED(ServerType) ->
      ?LOG_TOPOLOGY_ERROR(sharded, ServerType),
      {configured_mongo_type_mismatch, TopologyType, ServerType};

    replicaSetNoPrimary when ?STAL_MONGS(ServerType) ->
      ?LOG_TOPOLOGY_ERROR(replicaSetNoPrimary, ServerType),
      {configured_mongo_type_mismatch, TopologyType, ServerType};

    replicaSetNoPrimary when ?SEC_ARB_OTH(ServerType) andalso ServerSetName /= TopologySetName andalso TopologySetName /= undefined ->
      ?LOG_SET_NAME_ERROR(TopologySetName, ServerSetName),
      {configured_mongo_set_name_mismatch, TopologySetName, ServerSetName};

    replicaSetWithPrimary when ?STAL_MONGS(ServerType) ->
      ?LOG_TOPOLOGY_ERROR(replicaSetWithPrimary, ServerType),
      {configured_mongo_type_mismatch, TopologyType, ServerType};

    replicaSetWithPrimary when ?SEC_ARB_OTH(ServerType) andalso ServerSetName /= TopologySetName ->
      ?LOG_SET_NAME_ERROR(TopologySetName, ServerSetName),
      {configured_mongo_set_name_mismatch, TopologySetName, ServerSetName};

    _ when ServerType == rsPrimary andalso ServerSetName /= TopologySetName ->
      ?LOG_SET_NAME_ERROR(TopologySetName, ServerSetName),
      {configured_mongo_set_name_mismatch, TopologySetName, ServerSetName};
    _ ->
      ok
  end.

server_type(#{<<"ismaster">> := true, <<"secondary">> := false, <<"setName">> := _}) ->
  rsPrimary;
server_type(#{<<"isWritablePrimary">> := true, <<"secondary">> := false, <<"setName">> := _}) ->
  rsPrimary;
server_type(#{<<"ismaster">> := false, <<"secondary">> := true, <<"setName">> := _}) ->
  rsSecondary;
server_type(#{<<"isWritablePrimary">> := false, <<"secondary">> := true, <<"setName">> := _}) ->
  rsSecondary;
server_type(#{<<"arbiterOnly">> := true, <<"setName">> := _}) ->
  rsArbiter;
server_type(#{<<"hidden">> := true, <<"setName">> := _}) ->
  rsOther;
server_type(#{<<"setName">> := _}) ->
  rsOther;
server_type(#{<<"msg">> := <<"isdbgrid">>}) ->
  mongos;
server_type(#{<<"isreplicaset">> := true}) ->
  rsGhost;
server_type(#{<<"isWritablePrimary">> := true}) ->
  standalone;
server_type(#{<<"ok">> := _}) ->
  unknown;
server_type(_) ->
  standalone.

%% @private
start_seed([], Host, Tab, Topts, Wopts) ->
  {ok, Pid} = mc_server:start(self(), Host, Topts, Wopts),
  MRef = erlang:monitor(process, Pid),
  ets:insert(Tab, #mc_server{pid = Pid, mref = MRef, host = Host});
start_seed(_, _, _, _, _) ->
  ok.

%% @private
set_possible_primary(_, undefined) -> ok;
set_possible_primary(Tab, Addr) ->
  Saved = ets:select(Tab, [{#mc_server{host = Addr, type = unknown, _ = '_'}, [], ['$_']}]),
  update_possible_primary(Tab, Saved).

%% @private
update_possible_primary(_, []) -> ok;
update_possible_primary(Tab, [Saved]) ->
  ets:insert(Tab, Saved#mc_server{type = possiblePrimary}).

%% @private
checkIfHasPrimary(Tab) ->
  Res = ets:select(Tab, [{#mc_server{type = rsPrimary, _ = '_'}, [], ['$_']}]),
  checkIfHasPrimary_Res(Res).

%% @private
checkIfHasPrimary_Res([]) ->
  replicaSetNoPrimary;
checkIfHasPrimary_Res(_) ->
  replicaSetWithPrimary.
