%% API for standalone mongo client. You get connection pid of gen_server via connect/2
%% and then pass it to all functions

-module(mc_worker_api).

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").

-export([
  connect/1,
  disconnect/1,
  insert/3,
  update/4,
  update/6,
  delete/3,
  delete_one/3,
  delete_limit/4,
  insert/4,
  update/7,
  delete_limit/5]).

-export([
  find_one/3,
  find_one/4,
  find/3,
  find/4,
  find/2,
  find_one/2]).
-export([
  count/3,
  count/4,
  count/2
  ]).
-export([
  command/2,
  command/3,
  sync_command/4,
  ensure_index/3,
  prepare/2]).

-define(START_WORKER_TIMEOUT, 30000).

%% @doc Make one connection to server, return its pid
-spec connect(args()) -> {ok, pid()} | {error, Reason :: term()}.
connect(Args) ->
  case mc_worker:start_link([{parent, self()} | Args]) of
    {ok, Pid} ->
      StartWorkerTimeout = mc_utils:get_value(start_worker_timeout, Args, ?START_WORKER_TIMEOUT),
      case mc_util:wait_connect_complete(1, StartWorkerTimeout) of
        ok -> {ok, Pid};
        {error, _} = Err -> Err
      end;
    {error, _} = Err -> Err
  end.

-spec disconnect(pid()) -> ok.
disconnect(Connection) ->
  mc_worker:disconnect(Connection).

%% @doc Insert a document or multiple documents into a collection.
%%      Returns the document or documents with an auto-generated _id if missing.
-spec insert(pid(), collection(), list() | map() | bson:document()) -> {{boolean(), map()}, list()}.
insert(Connection, Coll, Docs) ->
  insert(Connection, Coll, Docs, {<<"w">>, 1}).

-spec insert(pid(), collection(), list() | map() | bson:document(), bson:document()) -> {{boolean(), map()}, list()}.
insert(Connection, Coll, Doc, WC) when is_tuple(Doc); is_map(Doc) ->
  {Res, [UDoc | _]} = insert(Connection, Coll, [Doc], WC),
  {Res, UDoc};
insert(Connection, Coll, Docs, WriteConcern) ->
  Converted = prepare(Docs, fun assign_id/1),
  case mc_utils:use_legacy_protocol(Connection) of
      true -> 
          {command(Connection, 
                   {<<"insert">>, Coll,
                    <<"documents">>, Converted,
                    <<"writeConcern">>, WriteConcern}),
           Converted};
      false -> 
          Msg = #op_msg_write_op{command = insert,
                                 collection = Coll,
                                 extra_fields = [{<<"writeConcern">>, WriteConcern}],
                                 documents = Converted},
          {mc_connection_man:op_msg(Connection, Msg), Converted}
  end.



%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), collection(), selector(), map()) -> {boolean(), map()}.
update(Connection, Coll, Selector, Doc) ->
  update(Connection, Coll, Selector, Doc, false, false).

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), collection(), selector(), map(), boolean(), boolean()) -> {boolean(), map()}.
update(Connection, Coll, Selector, Doc, Upsert, MultiUpdate) ->
  update(Connection, Coll, Selector, Doc, Upsert, MultiUpdate, {<<"w">>, 1}).

%% @doc Replace the document matching criteria entirely with the new Document.
-spec update(pid(), collection(), selector(), map(), boolean(), boolean(), bson:document()) -> {boolean(), map()}.
update(Connection, Coll, Selector, Doc, Upsert, MultiUpdate, WC) ->
  Converted = prepare(Doc, fun(D) -> D end),
  case mc_utils:use_legacy_protocol(Connection) of
      true -> 
          command(Connection, {<<"update">>, Coll, <<"updates">>,
                               [#{<<"q">> => Selector,
                                  <<"u">> => Converted,
                                  <<"upsert">> => Upsert,
                                  <<"multi">> => MultiUpdate}],
                               <<"writeConcern">>, WC});
      false -> 
          Msg = #op_msg_write_op{command = update,
                                 collection = Coll,
                                 extra_fields = [{<<"writeConcern">>, WC}],
                                 documents_name = <<"updates">>,
                                 documents = [#{<<"q">> => Selector,
                                                <<"u">> => Converted,
                                                <<"upsert">> => Upsert,
                                                <<"multi">> => MultiUpdate}]},
          mc_connection_man:op_msg(Connection, Msg)
  end.

%% @doc Delete selected documents
-spec delete(pid(), collection(), selector()) -> {boolean(), map()}.
delete(Connection, Coll, Selector) ->
  delete_limit(Connection, Coll, Selector, 0).

%% @doc Delete first selected document.
-spec delete_one(pid(), collection(), selector()) -> {boolean(), map()}.
delete_one(Connection, Coll, Selector) ->
  delete_limit(Connection, Coll, Selector, 1).

%% @doc Delete selected documents
-spec delete_limit(pid(), collection(), selector(), integer()) -> {boolean(), map()}.
delete_limit(Connection, Coll, Selector, N) ->
  case mc_utils:use_legacy_protocol(Connection) of
      true -> 
          command(Connection, {<<"delete">>, Coll, <<"deletes">>,
                               [#{<<"q">> => Selector, <<"limit">> => N}]});
      false -> 
          Msg = #op_msg_write_op{command = delete,
                                 collection = Coll,
                                 extra_fields = [{<<"writeConcern">>, {<<"w">>, 1}}],
                                 documents_name = <<"deletes">>,
                                 documents = [#{<<"q">> => Selector,
                                                <<"limit">> => 1}]},
          mc_connection_man:op_msg(Connection, Msg)
  end.



%% @doc Delete selected documents
-spec delete_limit(pid(), collection(), selector(), integer(), bson:document()) -> {boolean(), map()}.
delete_limit(Connection, Coll, Selector, N, WC) ->
  command(Connection, {<<"delete">>, Coll, <<"deletes">>,
    [#{<<"q">> => Selector, <<"limit">> => N}], <<"writeConcern">>, WC}).

%% @doc Return first selected document, if any
-spec find_one(pid(), colldb(), selector()) -> map() | undefined.
find_one(Connection, Coll, Selector) ->
  find_one(Connection, Coll, Selector, #{}).

%% @doc Return first selected document, if any
-spec find_one(pid(), colldb(), selector(), map()) -> map() | undefined.
find_one(Connection, Coll, Selector, Args) ->
      Projector = maps:get(projector, Args, #{}),
      Skip = maps:get(skip, Args, 0),
      ReadPref = maps:get(readopts, Args, #{<<"mode">> => <<"primary">>}),
      case mc_utils:use_legacy_protocol(Connection) of
          true -> 
              SelectorWithReadPref = mongoc:append_read_preference(Selector, ReadPref),
              find_one(Connection,
                       #'query'{
                          collection = Coll,
                          selector = SelectorWithReadPref,
                          projector = Projector,
                          skip = Skip
                         });
          false -> 
              CommandDoc = [
                                {<<"find">>, Coll},
                                {<<"$readPreference">>, ReadPref},
                                {<<"filter">>, Selector},
                                {<<"projection">>, Projector},
                                {<<"skip">>, Skip},
                                {<<"batchSize">>, 1},
                                {<<"limit">>, 1},
                                {<<"singleBatch">>, true} %% Close cursor after first batch
                                        
                           ],
              mc_connection_man:op_msg_read_one(Connection,
                                                #'op_msg_command'{
                                                   command_doc = CommandDoc
                                                  })
      end.

-spec find_one(pid() | atom(), query()) -> map() | undefined.
find_one(Connection, Query) when is_record(Query, query) ->
    case mc_utils:use_legacy_protocol(Connection) of
        true -> mc_connection_man:read_one(Connection, Query);
        false ->
            #'query'{collection = Coll,
                     skip = Skip,
                     selector = Selector,
                     projector = Projector} = Query,
            {RP, NewSelector, _} = mongoc:extract_read_preference(Selector),
            Args = #{projector => Projector,
                     skip => Skip,
                     readopts => RP},
            find_one(Connection, Coll, NewSelector, Args)
    end.

%% @doc Return selected documents.
-spec find(pid(), colldb(), selector()) -> {ok, cursor()} | [].
find(Connection, Coll, Selector) ->
  find(Connection, Coll, Selector, #{}).

%% @doc Return projection of selected documents.
%%      Empty projection [] means full projection.
-spec find(pid(), colldb(), selector(), map()) -> {ok, cursor()} | [].
find(Connection, Coll, Selector, Args) ->
  Projector = maps:get(projector, Args, #{}),
  Skip = maps:get(skip, Args, 0),
  BatchSize = 
        case mc_utils:use_legacy_protocol(Connection) of
            true ->
                maps:get(batchsize, Args, 0);
            false ->
                maps:get(batchsize, Args, 101)
        end,
  ReadPref = maps:get(readopts, Args, #{<<"mode">> => <<"primary">>}),
  find(Connection,
    #'query'{
      collection = Coll,
      selector = mongoc:append_read_preference(Selector, ReadPref),
      projector = Projector,
      skip = Skip,
      batchsize = BatchSize,
      slaveok = true,
      sok_overriden = true
    }).

-spec find(pid() | atom(), query()) -> {ok, cursor()} | [].
find(Connection, Query) when is_record(Query, query) ->
    FixedQuery =
        case mc_utils:use_legacy_protocol(Connection) of
            true -> Query;
            false ->
                #'query'{collection = Coll,
                         skip = Skip,
                         selector = Selector,
                         batchsize = BatchSize,
                         projector = Projector} = Query,
                {ReadPref, NewSelector, OrderBy} = mongoc:extract_read_preference(Selector),
                %% We might need to do some transformations:
                %% See: https://github.com/mongodb/specifications/blob/master/source/find_getmore_killcursors_commands.rst#mapping-op-query-behavior-to-the-find-command-limit-and-batchsize-fields
                SingleBatch = BatchSize < 0,
                BatchSize2 = erlang:abs(BatchSize),
                BatchSizeField =
                    case BatchSize2 =:= 0 of
                        true -> [];
                        false -> [{<<"batchSize">>, BatchSize2}] 
                    end,
                SingleBatchField =
                    case SingleBatch of
                        true -> [];
                        false -> [{<<"singleBatch">>, SingleBatch}] 
                    end,
                SortField =
                    case OrderBy of
                        M when is_map(M), map_size(M) =:= 0 ->
                            [];
                        _ ->
                            [{<<"sort">>, OrderBy}] 
                    end,
                CommandDoc = [
                              {<<"find">>, Coll},
                              {<<"$readPreference">>, ReadPref},
                              {<<"filter">>, NewSelector},
                              {<<"projection">>, Projector},
                              {<<"skip">>, Skip}
                             ] ++ SortField
                               ++ BatchSizeField
                               ++ SingleBatchField,
                #op_msg_command{command_doc = CommandDoc} 
        end,
  case mc_connection_man:read(Connection, FixedQuery) of
    [] -> [];
    {ok, Cursor} when is_pid(Cursor) ->
      {ok, Cursor}
  end.

%% @doc Count selected documents
-spec count(pid(), collection(), selector()) -> integer().
count(Connection, Coll, Selector) ->
  count(Connection, Coll, Selector, #{}).

%% @doc Count selected documents up to given max number; 0 means no max.
%%     Ie. stops counting when max is reached to save processing time.
-spec count(pid(), collection(), selector(), map()) -> integer().
count(Connection, Coll, Selector, Args = #{limit := Limit}) when Limit > 0 ->
  ReadPref = maps:get(readopts, Args, #{<<"mode">> => <<"primary">>}),
  count(Connection, {<<"count">>, Coll, <<"query">>, Selector, <<"limit">>, Limit, <<"$readPreference">>, ReadPref});
count(Connection, Coll, Selector, Args) ->
  ReadPref = maps:get(readopts, Args, #{<<"mode">> => <<"primary">>}),
  count(Connection, {<<"count">>, Coll, <<"query">>, Selector, <<"$readPreference">>, ReadPref}).

-spec count(pid() | atom(), bson:document()) -> integer().
count(Connection, Query) ->
  {true, #{<<"n">> := N}} = command(Connection, Query),
  trunc(N). % Server returns count as float

%% @doc Create index on collection according to given spec. This function does
%% not work if you have configured the driver to use the new version of the
%% protocol with application:set_env(mongodb, use_legacy_protocol, false). In
%% that case you can call the createIndexes
%% (https://www.mongodb.com/docs/manual/reference/command/createIndexes/#mongodb-dbcommand-dbcmd.createIndexes)
%% command using the `mc_worker_api:command/2` function instead. 
%%
%%      The key specification is a bson documents with the following fields:
%%      IndexSpec      :: bson document, for e.g. {field, 1, other, -1, location, 2d}, <strong>required</strong>
-spec ensure_index(pid(), colldb(), bson:document()) -> ok | {error, any()}.
ensure_index(Connection, Coll, IndexSpec) ->
    case mc_utils:use_legacy_protocol(Connection) of
        true ->
            mc_connection_man:request_worker(Connection,
                                             #ensure_index{collection = Coll,
                                                           index_spec = IndexSpec});
        false -> 
           {error, <<"This function does not work when one have specified application:set_env(mongodb, use_legacy_protocol, false). Call the createIndexes command using mc_worker_api:command/2 instead.">>} 
    end.

%% @doc Execute given MongoDB command and return its result.
-spec command(pid(), mc_worker_api:selector()) -> {boolean(), map()}. % Action
command(Connection, Query) when is_record(Query, query) ->
      case mc_utils:use_legacy_protocol(Connection) of
          true -> 
              Doc = mc_connection_man:read_one(Connection, Query),
              mc_connection_man:process_reply(Doc, Query);
          false ->
              %% We will convert the legacy command to a modern one.
              #query{
                 slaveok = SlaveOk,
                 selector = Selector} = Query,
              Fields = bson:fields(Selector),
              NewSelector =
              case {lists:keyfind(<<"$readPreference">>, 1, Fields), SlaveOk} of
                  {{<<"$readPreference">>, _}, _} -> Selector;
                  {false, true} -> 
                      bson:document(Fields ++ [{<<"$readPreference">>, #{<<"mode">> => <<"primaryPreferred">>}}]);
                  {false, false} ->
                      %% primary is the default mode so we do not need to change anything
                      Selector
              end,    
              command(Connection, NewSelector)
      end;
command(Connection, Command) when is_tuple(Command) ->
  case mc_utils:use_legacy_protocol(Connection) of
      true -> 
          command(Connection,
                  #'query'{
                     collection = <<"$cmd">>,
                     selector = Command
                    });
      false ->
          command(Connection, bson:fields(Command))
  end;
command(Connection, Command) when is_list(Command) ->
  case mc_utils:use_legacy_protocol(Connection) of
      true -> 
          command(Connection, bson:document(Command));
      false ->
          Msg = #op_msg_command{command_doc = fix_command_obj_list(Command)},
          {true, mc_connection_man:op_msg_raw_result(Connection, Msg)}
  end;
command(Connection, Command) when is_map(Command) ->
    command(Connection, map_to_command(Command)).

%% Converts map to a bson fields list. Makes sure that the command is placed
%% first as this is a requirement from MongoDB
map_to_command(Command) ->
    case lists:search(fun is_command/1, maps:keys(Command)) of
        {value, Key} ->
            CommandValue = maps:get(Key, Command),
            NewCommand1 = maps:remove(Key, Command),
            NewCommand2 = maps:to_list(NewCommand1, NewCommand1),
            [{Key, CommandValue} | NewCommand2];
        false ->
            maps:to_list(Command)
    end.

fix_command_obj_list(Map) when is_map(Map) ->
    fix_command_obj_list(maps:to_list(Map));
fix_command_obj_list(Tuple) when is_tuple(Tuple) ->
    fix_command_obj_list(bson:fields(Tuple));
fix_command_obj_list(List) when is_list(List) ->
    %% we have to try to figure out what the command field is and put it first as the command field need to go first
    List.

command(Connection, Command, _IsSlaveOk = true) ->
    case mc_utils:use_legacy_protocol(Connection) of
        true -> 
            command(Connection,
                    #'query'{
                       collection = <<"$cmd">>,
                       selector = Command,
                       slaveok = true,
                       sok_overriden = true
                      });
        false ->
            Command = fix_command_obj_list(Command),

            %% slaveok seems to correspond to primaryPreferred in the new protocol
            CommandExtened = Command ++ [{<<"$readPreference">>, #{<<"mode">> => <<"primaryPreferred">>}}],
            command(Connection, CommandExtened)
    end;
command(Connection, Command, _IsSlaveOk = false) ->
  command(Connection, Command).

%% @doc Execute MongoDB command in this thread
-spec sync_command(socket(), binary(), mc_worker_api:selector(), module()) -> {boolean(), map()}.
sync_command(Socket, Database, Command, SetOpts) ->
    case true of %% TODO mc_utils:use_legacy_protocol(Connection)
        true -> 
            Doc = mc_connection_man:read_one_sync(Socket,
                                                  Database,
                                                  #'query'{
                                                     collection = <<"$cmd">>,
                                                     selector = Command
                                                    },
                                                  SetOpts),
            mc_connection_man:process_reply(Doc, Command);
        false ->
            Request = #op_msg_command{command_doc = fix_command_obj_list(Command)},
            {_, [Doc]} = mc_connection_man:op_msg_sync(Socket, Database, Request, SetOpts),
            mc_connection_man:process_reply(Doc, Command)
    end.

-spec prepare(tuple() | list() | map(), fun()) -> list().
prepare(Docs, AssignFun) when is_tuple(Docs) -> %bson
  case element(1, Docs) of
    <<"$", _/binary>> -> Docs;  %command
    _ ->  %document
      case prepare_doc(Docs, AssignFun) of
        Res when is_tuple(Res) -> [Res];
        List -> List
      end
  end;
prepare(Doc, AssignFun) when is_map(Doc), map_size(Doc) == 1 ->
  case maps:keys(Doc) of
    [<<"$", _/binary>>] -> Doc; %command
    _ ->  %document
      case prepare_doc(Doc, AssignFun) of
        Res when is_tuple(Res) -> [Res];
        List -> List
      end
  end;
prepare(Doc, AssignFun) when is_map(Doc) ->
  Keys = maps:keys(Doc),
  case [K || <<"$", _/binary>> = K <- Keys] of
    Keys -> Doc; % multiple commands
    _ ->  % document
      case prepare_doc(Doc, AssignFun) of
        Res when is_tuple(Res) -> [Res];
        List -> List
      end
  end;
prepare(Docs, AssignFun) when is_list(Docs) ->
  case prepare_doc(Docs, AssignFun) of
    Res when not is_list(Res) -> [Res];
    List -> List
  end.


%% @private
%% Convert maps or proplists to bson
prepare_doc(Docs, AssignFun) when is_list(Docs) ->  %list of documents
  case mc_utils:is_proplist(Docs) of
    true -> prepare_doc(maps:from_list(Docs), AssignFun); %proplist
    false -> lists:map(fun(Doc) -> prepare_doc(Doc, AssignFun) end, Docs)
  end;
prepare_doc(Doc, AssignFun) ->
  AssignFun(Doc).

%% @private
-spec assign_id(bson:document() | map()) -> bson:document().
assign_id(Map) when is_map(Map) ->
  case maps:is_key(<<"_id">>, Map) of
    true -> Map;
    false -> Map#{<<"_id">> => mongo_id_server:object_id()}
  end;
assign_id(Doc) ->
  case bson:lookup(<<"_id">>, Doc) of
    {} -> bson:update(<<"_id">>, mongo_id_server:object_id(), Doc);
    _Value -> Doc
  end.

%% Source https://www.mongodb.com/docs/manual/reference/command/
%% Might need to get updated if commands are added
is_command(<<"aggregate">>) -> true;
is_command(aggregate) -> true;
is_command(<<"count">>) -> true;
is_command(count) -> true;
is_command(<<"distinct">>) -> true;
is_command(distinct) -> true;
is_command(<<"mapReduce">>) -> true;
is_command(mapReduce) -> true;
is_command(<<"geoSearch">>) -> true;
is_command(geoSearch) -> true;
is_command(<<"delete">>) -> true;
is_command(delete) -> true;
is_command(<<"find">>) -> true;
is_command(find) -> true;
is_command(<<"findAndModify">>) -> true;
is_command(findAndModify) -> true;
is_command(<<"getMore">>) -> true;
is_command(getMore) -> true;
is_command(<<"insert">>) -> true;
is_command(insert) -> true;
is_command(<<"resetError">>) -> true;
is_command(resetError) -> true;
is_command(<<"update">>) -> true;
is_command(update) -> true;
is_command(<<"planCacheClear">>) -> true;
is_command(planCacheClear) -> true;
is_command(<<"planCacheClearFilters">>) -> true;
is_command(planCacheClearFilters) -> true;
is_command(<<"planCacheListFilters">>) -> true;
is_command(planCacheListFilters) -> true;
is_command(<<"planCacheSetFilter">>) -> true;
is_command(planCacheSetFilter) -> true;
is_command(<<"authenticate">>) -> true;
is_command(authenticate) -> true;
is_command(<<"getnonce">>) -> true;
is_command(getnonce) -> true;
is_command(<<"logout">>) -> true;
is_command(logout) -> true;
is_command(<<"createUser">>) -> true;
is_command(createUser) -> true;
is_command(<<"dropAllUsersFromDatabase">>) -> true;
is_command(dropAllUsersFromDatabase) -> true;
is_command(<<"dropUser">>) -> true;
is_command(dropUser) -> true;
is_command(<<"grantRolesToUser">>) -> true;
is_command(grantRolesToUser) -> true;
is_command(<<"revokeRolesFromUser">>) -> true;
is_command(revokeRolesFromUser) -> true;
is_command(<<"updateUser">>) -> true;
is_command(updateUser) -> true;
is_command(<<"usersInfo">>) -> true;
is_command(usersInfo) -> true;
is_command(<<"createRole">>) -> true;
is_command(createRole) -> true;
is_command(<<"dropRole">>) -> true;
is_command(dropRole) -> true;
is_command(<<"dropAllRolesFromDatabase">>) -> true;
is_command(dropAllRolesFromDatabase) -> true;
is_command(<<"grantPrivilegesToRole">>) -> true;
is_command(grantPrivilegesToRole) -> true;
is_command(<<"grantRolesToRole">>) -> true;
is_command(grantRolesToRole) -> true;
is_command(<<"invalidateUserCache">>) -> true;
is_command(invalidateUserCache) -> true;
is_command(<<"revokePrivilegesFromRole">>) -> true;
is_command(revokePrivilegesFromRole) -> true;
is_command(<<"revokeRolesFromRole">>) -> true;
is_command(revokeRolesFromRole) -> true;
is_command(<<"rolesInfo">>) -> true;
is_command(rolesInfo) -> true;
is_command(<<"updateRole">>) -> true;
is_command(updateRole) -> true;
is_command(<<"applyOps">>) -> true;
is_command(applyOps) -> true;
is_command(<<"hello">>) -> true;
is_command(hello) -> true;
is_command(<<"replSetAbortPrimaryCatchUp">>) -> true;
is_command(replSetAbortPrimaryCatchUp) -> true;
is_command(<<"replSetFreeze">>) -> true;
is_command(replSetFreeze) -> true;
is_command(<<"replSetGetConfig">>) -> true;
is_command(replSetGetConfig) -> true;
is_command(<<"replSetGetStatus">>) -> true;
is_command(replSetGetStatus) -> true;
is_command(<<"replSetInitiate">>) -> true;
is_command(replSetInitiate) -> true;
is_command(<<"replSetMaintenance">>) -> true;
is_command(replSetMaintenance) -> true;
is_command(<<"replSetReconfig">>) -> true;
is_command(replSetReconfig) -> true;
is_command(<<"replSetResizeOplog">>) -> true;
is_command(replSetResizeOplog) -> true;
is_command(<<"replSetStepDown">>) -> true;
is_command(replSetStepDown) -> true;
is_command(<<"replSetSyncFrom">>) -> true;
is_command(replSetSyncFrom) -> true;
is_command(<<"abortReshardCollection">>) -> true;
is_command(abortReshardCollection) -> true;
is_command(<<"addShard">>) -> true;
is_command(addShard) -> true;
is_command(<<"addShardToZone">>) -> true;
is_command(addShardToZone) -> true;
is_command(<<"balancerCollectionStatus">>) -> true;
is_command(balancerCollectionStatus) -> true;
is_command(<<"balancerStart">>) -> true;
is_command(balancerStart) -> true;
is_command(<<"balancerStatus">>) -> true;
is_command(balancerStatus) -> true;
is_command(<<"balancerStop">>) -> true;
is_command(balancerStop) -> true;
is_command(<<"checkShardingIndex">>) -> true;
is_command(checkShardingIndex) -> true;
is_command(<<"clearJumboFlag">>) -> true;
is_command(clearJumboFlag) -> true;
is_command(<<"cleanupOrphaned">>) -> true;
is_command(cleanupOrphaned) -> true;
is_command(<<"cleanupReshardCollection">>) -> true;
is_command(cleanupReshardCollection) -> true;
is_command(<<"commitReshardCollection">>) -> true;
is_command(commitReshardCollection) -> true;
is_command(<<"configureCollectionBalancing">>) -> true;
is_command(configureCollectionBalancing) -> true;
is_command(<<"enableSharding">>) -> true;
is_command(enableSharding) -> true;
is_command(<<"flushRouterConfig">>) -> true;
is_command(flushRouterConfig) -> true;
is_command(<<"getShardMap">>) -> true;
is_command(getShardMap) -> true;
is_command(<<"getShardVersion">>) -> true;
is_command(getShardVersion) -> true;
is_command(<<"isdbgrid">>) -> true;
is_command(isdbgrid) -> true;
is_command(<<"listShards">>) -> true;
is_command(listShards) -> true;
is_command(<<"medianKey">>) -> true;
is_command(medianKey) -> true;
is_command(<<"moveChunk">>) -> true;
is_command(moveChunk) -> true;
is_command(<<"movePrimary">>) -> true;
is_command(movePrimary) -> true;
is_command(<<"mergeChunks">>) -> true;
is_command(mergeChunks) -> true;
is_command(<<"refineCollectionShardKey">>) -> true;
is_command(refineCollectionShardKey) -> true;
is_command(<<"removeShard">>) -> true;
is_command(removeShard) -> true;
is_command(<<"removeShardFromZone">>) -> true;
is_command(removeShardFromZone) -> true;
is_command(<<"reshardCollection">>) -> true;
is_command(reshardCollection) -> true;
is_command(<<"setShardVersion">>) -> true;
is_command(setShardVersion) -> true;
is_command(<<"shardCollection">>) -> true;
is_command(shardCollection) -> true;
is_command(<<"shardingState">>) -> true;
is_command(shardingState) -> true;
is_command(<<"split">>) -> true;
is_command(split) -> true;
is_command(<<"splitVector">>) -> true;
is_command(splitVector) -> true;
is_command(<<"unsetSharding">>) -> true;
is_command(unsetSharding) -> true;
is_command(<<"updateZoneKeyRange">>) -> true;
is_command(updateZoneKeyRange) -> true;
is_command(<<"abortTransaction">>) -> true;
is_command(abortTransaction) -> true;
is_command(<<"commitTransaction">>) -> true;
is_command(commitTransaction) -> true;
is_command(<<"endSessions">>) -> true;
is_command(endSessions) -> true;
is_command(<<"killAllSessions">>) -> true;
is_command(killAllSessions) -> true;
is_command(<<"killAllSessionsByPattern">>) -> true;
is_command(killAllSessionsByPattern) -> true;
is_command(<<"killSessions">>) -> true;
is_command(killSessions) -> true;
is_command(<<"refreshSessions">>) -> true;
is_command(refreshSessions) -> true;
is_command(<<"startSession">>) -> true;
is_command(startSession) -> true;
is_command(<<"cloneCollectionAsCapped">>) -> true;
is_command(cloneCollectionAsCapped) -> true;
is_command(<<"collMod">>) -> true;
is_command(collMod) -> true;
is_command(<<"compact">>) -> true;
is_command(compact) -> true;
is_command(<<"compactStructuredEncryptionData">>) -> true;
is_command(compactStructuredEncryptionData) -> true;
is_command(<<"convertToCapped">>) -> true;
is_command(convertToCapped) -> true;
is_command(<<"create">>) -> true;
is_command(create) -> true;
is_command(<<"createIndexes">>) -> true;
is_command(createIndexes) -> true;
is_command(<<"currentOp">>) -> true;
is_command(currentOp) -> true;
is_command(<<"drop">>) -> true;
is_command(drop) -> true;
is_command(<<"dropDatabase">>) -> true;
is_command(dropDatabase) -> true;
is_command(<<"dropConnections">>) -> true;
is_command(dropConnections) -> true;
is_command(<<"dropIndexes">>) -> true;
is_command(dropIndexes) -> true;
is_command(<<"filemd5">>) -> true;
is_command(filemd5) -> true;
is_command(<<"fsync">>) -> true;
is_command(fsync) -> true;
is_command(<<"fsyncUnlock">>) -> true;
is_command(fsyncUnlock) -> true;
is_command(<<"getDefaultRWConcern">>) -> true;
is_command(getDefaultRWConcern) -> true;
is_command(<<"getClusterParameter">>) -> true;
is_command(getClusterParameter) -> true;
is_command(<<"getParameter">>) -> true;
is_command(getParameter) -> true;
is_command(<<"killCursors">>) -> true;
is_command(killCursors) -> true;
is_command(<<"killOp">>) -> true;
is_command(killOp) -> true;
is_command(<<"listCollections">>) -> true;
is_command(listCollections) -> true;
is_command(<<"listDatabases">>) -> true;
is_command(listDatabases) -> true;
is_command(<<"listIndexes">>) -> true;
is_command(listIndexes) -> true;
is_command(<<"logRotate">>) -> true;
is_command(logRotate) -> true;
is_command(<<"reIndex">>) -> true;
is_command(reIndex) -> true;
is_command(<<"renameCollection">>) -> true;
is_command(renameCollection) -> true;
is_command(<<"rotateCertificates">>) -> true;
is_command(rotateCertificates) -> true;
is_command(<<"setFeatureCompatibilityVersion">>) -> true;
is_command(setFeatureCompatibilityVersion) -> true;
is_command(<<"setIndexCommitQuorum">>) -> true;
is_command(setIndexCommitQuorum) -> true;
is_command(<<"setClusterParameter">>) -> true;
is_command(setClusterParameter) -> true;
is_command(<<"setParameter">>) -> true;
is_command(setParameter) -> true;
is_command(<<"setDefaultRWConcern">>) -> true;
is_command(setDefaultRWConcern) -> true;
is_command(<<"shutdown">>) -> true;
is_command(shutdown) -> true;
is_command(<<"buildInfo">>) -> true;
is_command(buildInfo) -> true;
is_command(<<"collStats">>) -> true;
is_command(collStats) -> true;
is_command(<<"connPoolStats">>) -> true;
is_command(connPoolStats) -> true;
is_command(<<"connectionStatus">>) -> true;
is_command(connectionStatus) -> true;
is_command(<<"dataSize">>) -> true;
is_command(dataSize) -> true;
is_command(<<"dbHash">>) -> true;
is_command(dbHash) -> true;
is_command(<<"dbStats">>) -> true;
is_command(dbStats) -> true;
is_command(<<"driverOIDTest">>) -> true;
is_command(driverOIDTest) -> true;
is_command(<<"explain">>) -> true;
is_command(explain) -> true;
is_command(<<"features">>) -> true;
is_command(features) -> true;
is_command(<<"getCmdLineOpts">>) -> true;
is_command(getCmdLineOpts) -> true;
is_command(<<"getLog">>) -> true;
is_command(getLog) -> true;
is_command(<<"hostInfo">>) -> true;
is_command(hostInfo) -> true;
is_command(<<"_isSelf">>) -> true;
is_command('_isSelf') -> true;
is_command(<<"listCommands">>) -> true;
is_command(listCommands) -> true;
is_command(<<"lockInfo">>) -> true;
is_command(lockInfo) -> true;
is_command(<<"netstat">>) -> true;
is_command(netstat) -> true;
is_command(<<"ping">>) -> true;
is_command(ping) -> true;
is_command(<<"profile">>) -> true;
is_command(profile) -> true;
is_command(<<"serverStatus">>) -> true;
is_command(serverStatus) -> true;
is_command(<<"shardConnPoolStats">>) -> true;
is_command(shardConnPoolStats) -> true;
is_command(<<"top">>) -> true;
is_command(top) -> true;
is_command(<<"validate">>) -> true;
is_command(validate) -> true;
is_command(<<"whatsmyuri">>) -> true;
is_command(whatsmyuri) -> true;
is_command(<<"setFreeMonitoring">>) -> true;
is_command(setFreeMonitoring) -> true;
is_command(<<"logApplicationMessage">>) -> true;
is_command(logApplicationMessage) -> true;
is_command(_) -> false.
