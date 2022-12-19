%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%% Api helper module. You can use it as an example for your own api
%%% to mongoc, as not all parameters are passed.
%%% @end
%%% Created : 19. Jan 2016 16:04
%%%-------------------------------------------------------------------
-module(mongo_api).
-author("tihon").

-include("mongoc.hrl").
-include("mongo_protocol.hrl").

-type transaction_result(T) :: T | {error, term()}.

%% API
-export([
  connect/4,
  insert/3,
  find/4,
  find/6,
  find_one/4,
  find_one/5,
  find_one/6,
  update/5,
  delete/3,
  count/4,
  command/3,
  ensure_index/3,
  disconnect/1]).

-spec connect(atom()|{atom(), binary()}, list(), proplists:proplist(), proplists:proplist()) -> {ok, pid()}.
connect(Type, Hosts, TopologyOptions, WorkerOptions) when is_atom(Type) ->
  mongoc:connect({Type, Hosts}, TopologyOptions, WorkerOptions);
connect(Type, Hosts, TopologyOptions, WorkerOptions) ->
  mongoc:connect(erlang:append_element(Type, Hosts), TopologyOptions, WorkerOptions).

-spec insert(atom() | pid(), collection(), list() | map() | bson:document()) ->
  transaction_result({{boolean(), map()}, list()}).
insert(Topology, Collection, Document) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:insert(Worker, Collection, Document)
    end,
    #{}).

-spec update(atom() | pid(), collection(), selector(), map(), map()) ->
  transaction_result({boolean(), map()}).
update(Topology, Collection, Selector, Doc, Opts) ->
  Upsert = maps:get(upsert, Opts, false),
  MultiUpdate = maps:get(multi, Opts, false),
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:update(Worker, Collection, Selector, Doc, Upsert, MultiUpdate)
    end, Opts).

-spec delete(atom() | pid(), collection(), selector()) ->
  transaction_result({boolean(), map()}).
delete(Topology, Collection, Selector) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:delete(Worker, Collection, Selector)
    end,
    #{}).

-spec find(atom() | pid(), collection(), selector(), projector()) ->
  transaction_result({ok, cursor()} | []).
find(Topology, Collection, Selector, Projector) ->
  find(Topology, Collection, Selector, Projector, 0, 0).

-spec find(atom() | pid(), collection(), selector(), projector(), integer(), integer()) ->
  transaction_result({ok, cursor()} | []).
find(Topology, Collection, Selector, Projector, Skip, Batchsize) ->
  mongoc:transaction_query(Topology,
    fun(Conf = #{pool := Worker}) ->
      Query = mongoc:find_query(Conf, Collection, Selector, Projector, Skip, Batchsize),
      mc_worker_api:find(Worker, Query)
    end, #{}).

-spec find_one(atom() | pid(), collection(), selector(), projector()) ->
  transaction_result(map() | undefined).
find_one(Topology, Collection, Selector, Projector) ->
  find_one(Topology, Collection, Selector, Projector, 0).

-spec find_one(atom() | pid(), collection(), selector(), projector(), integer()) ->
  transaction_result(map() | undefined).
find_one(Topology, Collection, Selector, Projector, Skip) ->
  find_one(Topology, Collection, Selector, Projector, Skip, ?TRANSACTION_TIMEOUT).

-spec find_one(atom() | pid(), collection(), selector(), projector(), integer(), timeout()) ->
  transaction_result(map() | undefined).
find_one(Topology, Collection, Selector, Projector, Skip, Timeout) ->
  mongoc:transaction_query(Topology,
    fun(Conf = #{pool := Worker}) ->
      Query = mongoc:find_one_query(Conf, Collection, Selector, Projector, Skip),
      mc_worker_api:find_one(Worker, Query)
    end, #{}, Timeout).

-spec count(atom() | pid(), collection(), selector(), integer()) ->
    transaction_result(integer()).
count(Topology, Collection, Selector, Limit) ->
  mongoc:transaction_query(Topology,
    fun(Conf = #{pool := Worker}) ->
      Query = mongoc:count_query(Conf, Collection, Selector, Limit),
      mc_worker_api:count(Worker, Query)
    end,
    #{}).

-spec command(atom() | pid(), selector(), timeout()) -> transaction_result(integer()).
command(Topology, Command, Timeout) ->
  mongoc:transaction_query(Topology,
    fun(Conf = #{pool := Worker}) ->
      Query = mongoc:command_query(Conf, Command),
      mc_worker_api:command(Worker, Query)
    end, #{}, Timeout).

%% @doc Creates index on collection according to given spec. This function does
%% not work if you have configured the driver to use the new version of the
%% protocol with application:set_env(mongodb, use_legacy_protocol, false). In
%% that case you can call the createIndexes
%% (https://www.mongodb.com/docs/manual/reference/command/createIndexes/#mongodb-dbcommand-dbcmd.createIndexes)
%% command using the `mc_worker_api:command/2` function instead. 
%%
%%      The key specification is a bson documents with the following fields:
%%      IndexSpec      :: bson document, for e.g. {field, 1, other, -1, location, 2d}, <strong>required</strong>
%%      The key specification is a bson documents with the following fields:
%%      key      :: bson document, for e.g. {field, 1, other, -1, location, 2d}, <strong>required</strong>
-spec ensure_index(pid() | atom(), collection(), bson:document()) -> transaction_result(ok).
ensure_index(Topology, Coll, IndexSpec) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:ensure_index(Worker, Coll, IndexSpec)
    end, #{}).

-spec disconnect(atom() | pid()) -> ok.
disconnect(Topology) ->
  mongoc:disconnect(Topology).
