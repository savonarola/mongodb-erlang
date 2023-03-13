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
  sync_command/5,
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
                                                <<"limit">> => N}]},
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
            NewCommand2 = maps:to_list(NewCommand1),
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
%%
%% This function uses the legacy protocol (without OP_MSG packages) that was
%% removed in Mongo DB 5.1. Use sync_command(Socket, Database, Command,
%% SetOpts, false) for the corresponding function that sends commands using the
%% OP_MSG based protocol. 
-spec sync_command(socket(), binary(), mc_worker_api:selector(), module()) -> {boolean(), map()}.
sync_command(Socket, Database, Command, SetOpts) ->
    sync_command(Socket, Database, Command, SetOpts, true).

sync_command(Socket, Database, Command, SetOpts, UseLegacyProtocol) ->
    case UseLegacyProtocol of
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
            Doc = mc_connection_man:op_msg_sync(Socket, Database, Request, SetOpts),
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
%% Should be updated if commands are added
is_command(Atom) when is_atom(Atom) ->
    is_command_bin(erlang:atom_to_binary(Atom));
is_command(Bin) when is_binary(Bin) ->
    is_command_bin(Bin).

is_command_bin(<<"aggregate">>) -> true;
is_command_bin(<<"count">>) -> true;
is_command_bin(<<"distinct">>) -> true;
is_command_bin(<<"mapReduce">>) -> true;
is_command_bin(<<"geoSearch">>) -> true;
is_command_bin(<<"delete">>) -> true;
is_command_bin(<<"find">>) -> true;
is_command_bin(<<"findAndModify">>) -> true;
is_command_bin(<<"getMore">>) -> true;
is_command_bin(<<"insert">>) -> true;
is_command_bin(<<"resetError">>) -> true;
is_command_bin(<<"update">>) -> true;
is_command_bin(<<"planCacheClear">>) -> true;
is_command_bin(<<"planCacheClearFilters">>) -> true;
is_command_bin(<<"planCacheListFilters">>) -> true;
is_command_bin(<<"planCacheSetFilter">>) -> true;
is_command_bin(<<"authenticate">>) -> true;
is_command_bin(<<"getnonce">>) -> true;
is_command_bin(<<"logout">>) -> true;
is_command_bin(<<"createUser">>) -> true;
is_command_bin(<<"dropAllUsersFromDatabase">>) -> true;
is_command_bin(<<"dropUser">>) -> true;
is_command_bin(<<"grantRolesToUser">>) -> true;
is_command_bin(<<"revokeRolesFromUser">>) -> true;
is_command_bin(<<"updateUser">>) -> true;
is_command_bin(<<"usersInfo">>) -> true;
is_command_bin(<<"createRole">>) -> true;
is_command_bin(<<"dropRole">>) -> true;
is_command_bin(<<"dropAllRolesFromDatabase">>) -> true;
is_command_bin(<<"grantPrivilegesToRole">>) -> true;
is_command_bin(<<"grantRolesToRole">>) -> true;
is_command_bin(<<"invalidateUserCache">>) -> true;
is_command_bin(<<"isMaster">>) -> true;
is_command_bin(<<"revokePrivilegesFromRole">>) -> true;
is_command_bin(<<"revokeRolesFromRole">>) -> true;
is_command_bin(<<"rolesInfo">>) -> true;
is_command_bin(<<"updateRole">>) -> true;
is_command_bin(<<"applyOps">>) -> true;
is_command_bin(<<"hello">>) -> true;
is_command_bin(<<"replSetAbortPrimaryCatchUp">>) -> true;
is_command_bin(<<"replSetFreeze">>) -> true;
is_command_bin(<<"replSetGetConfig">>) -> true;
is_command_bin(<<"replSetGetStatus">>) -> true;
is_command_bin(<<"replSetInitiate">>) -> true;
is_command_bin(<<"replSetMaintenance">>) -> true;
is_command_bin(<<"replSetReconfig">>) -> true;
is_command_bin(<<"replSetResizeOplog">>) -> true;
is_command_bin(<<"replSetStepDown">>) -> true;
is_command_bin(<<"replSetSyncFrom">>) -> true;
is_command_bin(<<"abortReshardCollection">>) -> true;
is_command_bin(<<"addShard">>) -> true;
is_command_bin(<<"addShardToZone">>) -> true;
is_command_bin(<<"balancerCollectionStatus">>) -> true;
is_command_bin(<<"balancerStart">>) -> true;
is_command_bin(<<"balancerStatus">>) -> true;
is_command_bin(<<"balancerStop">>) -> true;
is_command_bin(<<"checkShardingIndex">>) -> true;
is_command_bin(<<"clearJumboFlag">>) -> true;
is_command_bin(<<"cleanupOrphaned">>) -> true;
is_command_bin(<<"cleanupReshardCollection">>) -> true;
is_command_bin(<<"commitReshardCollection">>) -> true;
is_command_bin(<<"configureCollectionBalancing">>) -> true;
is_command_bin(<<"enableSharding">>) -> true;
is_command_bin(<<"flushRouterConfig">>) -> true;
is_command_bin(<<"getShardMap">>) -> true;
is_command_bin(<<"getShardVersion">>) -> true;
is_command_bin(<<"isdbgrid">>) -> true;
is_command_bin(<<"listShards">>) -> true;
is_command_bin(<<"medianKey">>) -> true;
is_command_bin(<<"moveChunk">>) -> true;
is_command_bin(<<"movePrimary">>) -> true;
is_command_bin(<<"mergeChunks">>) -> true;
is_command_bin(<<"refineCollectionShardKey">>) -> true;
is_command_bin(<<"removeShard">>) -> true;
is_command_bin(<<"removeShardFromZone">>) -> true;
is_command_bin(<<"reshardCollection">>) -> true;
is_command_bin(<<"setShardVersion">>) -> true;
is_command_bin(<<"shardCollection">>) -> true;
is_command_bin(<<"shardingState">>) -> true;
is_command_bin(<<"split">>) -> true;
is_command_bin(<<"splitVector">>) -> true;
is_command_bin(<<"unsetSharding">>) -> true;
is_command_bin(<<"updateZoneKeyRange">>) -> true;
is_command_bin(<<"abortTransaction">>) -> true;
is_command_bin(<<"commitTransaction">>) -> true;
is_command_bin(<<"endSessions">>) -> true;
is_command_bin(<<"killAllSessions">>) -> true;
is_command_bin(<<"killAllSessionsByPattern">>) -> true;
is_command_bin(<<"killSessions">>) -> true;
is_command_bin(<<"refreshSessions">>) -> true;
is_command_bin(<<"startSession">>) -> true;
is_command_bin(<<"cloneCollectionAsCapped">>) -> true;
is_command_bin(<<"collMod">>) -> true;
is_command_bin(<<"compact">>) -> true;
is_command_bin(<<"compactStructuredEncryptionData">>) -> true;
is_command_bin(<<"convertToCapped">>) -> true;
is_command_bin(<<"create">>) -> true;
is_command_bin(<<"createIndexes">>) -> true;
is_command_bin(<<"currentOp">>) -> true;
is_command_bin(<<"drop">>) -> true;
is_command_bin(<<"dropDatabase">>) -> true;
is_command_bin(<<"dropConnections">>) -> true;
is_command_bin(<<"dropIndexes">>) -> true;
is_command_bin(<<"filemd5">>) -> true;
is_command_bin(<<"fsync">>) -> true;
is_command_bin(<<"fsyncUnlock">>) -> true;
is_command_bin(<<"getDefaultRWConcern">>) -> true;
is_command_bin(<<"getClusterParameter">>) -> true;
is_command_bin(<<"getParameter">>) -> true;
is_command_bin(<<"killCursors">>) -> true;
is_command_bin(<<"killOp">>) -> true;
is_command_bin(<<"listCollections">>) -> true;
is_command_bin(<<"listDatabases">>) -> true;
is_command_bin(<<"listIndexes">>) -> true;
is_command_bin(<<"logRotate">>) -> true;
is_command_bin(<<"reIndex">>) -> true;
is_command_bin(<<"renameCollection">>) -> true;
is_command_bin(<<"rotateCertificates">>) -> true;
is_command_bin(<<"setFeatureCompatibilityVersion">>) -> true;
is_command_bin(<<"setIndexCommitQuorum">>) -> true;
is_command_bin(<<"setClusterParameter">>) -> true;
is_command_bin(<<"setParameter">>) -> true;
is_command_bin(<<"setDefaultRWConcern">>) -> true;
is_command_bin(<<"shutdown">>) -> true;
is_command_bin(<<"buildInfo">>) -> true;
is_command_bin(<<"collStats">>) -> true;
is_command_bin(<<"connPoolStats">>) -> true;
is_command_bin(<<"connectionStatus">>) -> true;
is_command_bin(<<"dataSize">>) -> true;
is_command_bin(<<"dbHash">>) -> true;
is_command_bin(<<"dbStats">>) -> true;
is_command_bin(<<"driverOIDTest">>) -> true;
is_command_bin(<<"explain">>) -> true;
is_command_bin(<<"features">>) -> true;
is_command_bin(<<"getCmdLineOpts">>) -> true;
is_command_bin(<<"getLog">>) -> true;
is_command_bin(<<"hostInfo">>) -> true;
is_command_bin(<<"_isSelf">>) -> true;
is_command_bin(<<"listCommands">>) -> true;
is_command_bin(<<"lockInfo">>) -> true;
is_command_bin(<<"netstat">>) -> true;
is_command_bin(<<"ping">>) -> true;
is_command_bin(<<"profile">>) -> true;
is_command_bin(<<"serverStatus">>) -> true;
is_command_bin(<<"shardConnPoolStats">>) -> true;
is_command_bin(<<"top">>) -> true;
is_command_bin(<<"validate">>) -> true;
is_command_bin(<<"whatsmyuri">>) -> true;
is_command_bin(<<"setFreeMonitoring">>) -> true;
is_command_bin(<<"logApplicationMessage">>) -> true;
is_command_bin(_) -> false.
