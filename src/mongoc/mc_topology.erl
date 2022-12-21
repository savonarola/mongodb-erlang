%%%-------------------------------------------------------------------
%%% @author Alexander Hudich (alttagil@gmail.com)
%%% @copyright (C) 2015, Alexander Hudich
%%% @doc
%%% mongoc internal module for monitoring a topology of one or more servers.
%%% @end
%%%-------------------------------------------------------------------
-module(mc_topology).
-author("alttagil@gmail.com").

-behaviour(gen_server).

-include("mongoc.hrl").
-include("mongo_logging.hrl").

%% API
-export([start_link/3, drop_server/2, update_topology/1, get_state/1, get_pool/2, get_pool/4, get_pool/5, get_pool/1, disconnect/1, get_state_part/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%===================================================================
%%% API
%%%===================================================================
-spec(start_link(any(), any(), any()) ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Seeds, TopologyOptions, WorkerOptions) ->
  gen_server:start_link(?MODULE, [Seeds, TopologyOptions, WorkerOptions], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
-spec(init(Args :: term()) ->
  {ok, State :: #topology_state{}} | {ok, State :: #topology_state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([SeedsList, TopologyOptions, WorkerOptions]) ->
  try_register(TopologyOptions),
  {Type, SetName, Seeds} = parse_seeds(SeedsList),
  ServerSelectionTimeoutMS = mc_utils:get_value(serverSelectionTimeoutMS, TopologyOptions, 30000),
  LocalThresholdMS = mc_utils:get_value(localThresholdMS, TopologyOptions, 200),
  RPmode = mc_utils:get_value(rp_mode, TopologyOptions, primary),
  RPTags = mc_utils:get_value(tags, TopologyOptions, []),
  GetPoolTimeout = mc_utils:get_value(get_pool_timeout, TopologyOptions, 5000),
  Servers = ets:new(mc_servers, [set, {keypos, 2}]),
  State = #topology_state{
    self = self(),
    servers = Servers,
    type = Type,
    rp_mode = RPmode,
    rp_tags = RPTags,
    seeds = Seeds,
    setName = SetName,
    serverSelectionTimeoutMS = ServerSelectionTimeoutMS,
    localThresholdMS = LocalThresholdMS,
    topology_opts = TopologyOptions,
    worker_opts = WorkerOptions,
    get_pool_timeout = GetPoolTimeout
  },
  gen_server:cast(self(), init_seeds),
  {Host, Port} = 
    case Seeds of
      [Seed|_] ->
        mc_util:parse_seed(Seed);
      Seed ->
        mc_util:parse_seed(Seed)
    end,
  ConnectTimeoutMS = proplists:get_value(connectTimeoutMS, TopologyOptions, 20000),
  ConnectArgs = mc_util:form_connect_args(Host, Port, ConnectTimeoutMS, WorkerOptions),
  ?DEBUG("Starting server and config validation: ~p", [{?SECURE(ConnectArgs), ConnectTimeoutMS, ?SECURE(WorkerOptions)}]),  
  case mc_topology_logics:validate_server_and_config(ConnectArgs, Type, SetName) of
    ok ->
      ?DEBUG("Server [~p] and config validation success", [?SECURE(ConnectArgs)]),  
      {ok, State};
    Error ->
      ?DEBUG("Server [~p] and config validation error: ~p", [?SECURE(ConnectArgs), Error]),  
      {stop, Error}
  end.

-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #topology_state{}) -> term()).
terminate(_Reason, _State) ->
  ok.

-spec(code_change(OldVsn :: term() | {down, term()}, State :: #topology_state{},
    Extra :: term()) ->
  {ok, NewState :: #topology_state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%%%===================================================================
%%% External functions
%%%===================================================================
disconnect(Topology) ->
  gen_server:cast(Topology, halt).

update_topology(Topology) ->
  gen_server:cast(Topology, update_topology).

get_state(Topology) ->
  gen_server:call(Topology, get_state).

get_state_part(Topology) ->
  gen_server:call(Topology, get_state_part).

get_pool(Topology) ->
  get_pool(Topology, #{}).

-spec get_pool(pid() | atom(), map() | list()) -> {ok, map()} | {error, any()}.
get_pool(Topology, Options) when is_list(Options) ->
  get_pool(Topology, maps:from_list(Options));
get_pool(Topology, Options) ->
  State = mc_topology:get_state(Topology),
  RPMode = maps:get(rp_mode, Options, State#topology_state.rp_mode),
  RPTags = maps:get(rp_tags, Options, State#topology_state.rp_tags),
  get_pool(RPMode, RPTags, State).

get_pool(RPMode, RPTags, State) ->
  TO = State#topology_state.topology_opts,
  ServerSelectionTimeoutMS = mc_utils:get_value(serverSelectionTimeoutMS, TO, 30000),
  Caller = self(),
  Pid = spawn(?MODULE, get_pool, [self(), State, RPMode, RPTags, Caller]),
  receive
    {Pid, {error, Reason}, _} ->
      {error, Reason};
    {Pid, Pool, Type} ->
      {ok, #{pool => Pool, server_type => Type, read_preference => #{mode => RPMode, tags => RPTags}}}
  after
    ServerSelectionTimeoutMS ->
      exit(Pid, timeout),
      mc_topology:update_topology(State#topology_state.self),
      {error, timeout}
  end.

get_pool(From, #topology_state{self = Topology, get_pool_timeout = TM} = State, RPMode, Tags) ->
  case mc_selecting_logics:select_server(Topology, RPMode, Tags) of
    #mc_server{pid = Pid, type = Type} ->
      Pool = mc_server:get_pool(Pid, TM),
      From ! {self(), Pool, Type};
    undefined ->
      timer:sleep(100),
      get_pool(From, State, RPMode, Tags)
  end.

get_pool(From, #topology_state{self = Topology, get_pool_timeout = TM} = State, RPMode, Tags, Caller) ->
  case is_process_alive(Caller) of
    false -> ok;
    true ->
      case mc_selecting_logics:select_server(Topology, RPMode, Tags) of
        #mc_server{pid = Pid, type = Type} ->
          Pool = mc_server:get_pool(Pid, TM),
          From ! {self(), Pool, Type};
        undefined ->
          timer:sleep(100),
          get_pool(From, State, RPMode, Tags, Caller)
      end
  end.


%%%===================================================================
%%% Handlers
%%%===================================================================

handle_call(get_state_part, _From, State = #topology_state{servers = Tab, type = TType, localThresholdMS = Threshold}) ->
  {reply, {Tab, TType, Threshold}, State};
handle_call(get_state, _From, State) ->
  {reply, State, State};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(halt, State) ->
  {stop, normal, State};
handle_cast(init_seeds, State) ->
  mc_topology_logics:init_seeds(State),
  {noreply, State};
handle_cast({monitor_ismaster, Server, IsMaster, RTT}, State) ->
  NState = parse_ismaster(Server, IsMaster, RTT, State),
  {noreply, NState};
handle_cast({server_to_unknown, Server}, State) ->
  NState = handle_server_to_unknown(Server, State),
  {noreply, NState};
handle_cast({drop_server, Pid}, State = #topology_state{servers = Tab}) ->
  ets:delete(Tab, Pid),
  {noreply, State};
handle_cast(update_topology, State = #topology_state{servers = Tab}) ->
  ets:foldl(
    fun(E, Acc) ->
      mc_monitor:next_loop(E#mc_server.pid),
      Acc
    end, [], Tab),
  {noreply, State};
handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(init_seeds, State) ->
  mc_topology_logics:init_seeds(State),
  {noreply, State};
handle_info({'DOWN', MRef, _, _, _}, State = #topology_state{servers = Tab}) ->
  case ets:match(Tab, #mc_server{pid = '$1', host = '$2', mref = MRef, _ = '_'}) of
    [[Pid, _Host]] ->
      true = ets:delete(Tab, Pid),
      erlang:send_after(1000, self(), init_seeds),
      {noreply, State};
    [] ->
      {noreply, State}
  end;
handle_info(_Info, State) ->
  {noreply, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================
%% @private
%% Register this process if needed
try_register(Options) ->
  case lists:keyfind(register, 1, Options) of
    false -> ok;
    {_, Name} when is_atom(Name) -> register(Name, self());
    {_, RegFun} when is_function(RegFun) -> RegFun(self())
  end.

%% @private
handle_server_to_unknown(Server, #topology_state{servers = Tab} = State) ->
  [Saved] = ets:select(Tab, [{#mc_server{pid = Server, _ = '_'}, [], ['$_']}]),
  ToUpdate = #mc_server{
    pid = Saved#mc_server.pid,
    mref = Saved#mc_server.mref,
    host = Saved#mc_server.host,
    type = unknown
  },
  ets:insert(Tab, ToUpdate),
  mc_server:update_unknown(ToUpdate#mc_server.pid),
  mc_topology_logics:update_topology_state(ToUpdate, State).

%% @private
parse_ismaster(Server, IsMaster, RTT, State = #topology_state{servers = Tab}) ->
  SType = mc_topology_logics:server_type(IsMaster),
  [Saved] = ets:select(Tab, [{#mc_server{pid = Server, _ = '_'}, [], ['$_']}]),
  {OldRTT, NRTT} = parse_rtt(Saved#mc_server.old_rtt, Saved#mc_server.rtt, RTT),
  ToUpdate = Saved#mc_server{
    type = SType,
    me = maps:get(<<"me">>, IsMaster, undefined),
    old_rtt = OldRTT,
    rtt = NRTT,
    setName = maps:get(<<"setName">>, IsMaster, undefined),
    setVersion = maps:get(<<"setVersion">>, IsMaster, undefined),
    tags = maps:get(<<"tags">>, IsMaster, []),
    minWireVersion = maps:get(<<"minWireVersion">>, IsMaster, 0),
    maxWireVersion = maps:get(<<"maxWireVersion">>, IsMaster, 2),
    hosts = maps:get(<<"hosts">>, IsMaster, []),
    passives = maps:get(<<"passives">>, IsMaster, []),
    arbiters = maps:get(<<"arbiters">>, IsMaster, []),
    electionId = maps:get(<<"electionId">>, IsMaster, undefined),
    primary = maps:get(<<"primary">>, IsMaster, undefined),
    ismaster = maps:get(<<"ismaster">>, IsMaster, undefined),
    secondary = maps:get(<<"secondary">>, IsMaster, undefined)
  },
  ets:insert(Tab, ToUpdate),
  mc_server:update_ismaster(ToUpdate#mc_server.pid, {SType, ToUpdate}),
  mc_topology_logics:update_topology_state(ToUpdate, State).

%% @private
parse_rtt(_, undefined, RTT) -> {RTT, RTT};
parse_rtt(OldRTT, CurRTT, RTT) ->
  A = 0.2,
  {CurRTT, A * RTT + (1 - A) * OldRTT / 1000}.

%% @private
drop_server(Topology, Server) ->
  gen_server:cast(Topology, {drop_server, Server}).

%% @private
parse_seeds({single, Addr}) -> {single, undefined, Addr};
parse_seeds({unknown, Seeds}) -> {unknown, undefined, Seeds};
parse_seeds({sharded, Seeds}) -> {sharded, undefined, Seeds};
parse_seeds({rs, SetName, Seeds}) -> {replicaSetNoPrimary, SetName, Seeds};
parse_seeds(Addr) when is_list(Addr) -> {single, undefined, [Addr]}.
