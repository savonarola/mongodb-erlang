-module(mc_worker_api_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("mongo_protocol.hrl").

-compile(nowarn_export_all).
-compile(export_all).

all() ->
  [
    insert_and_find,
    insert_and_delete,
    search_and_query,
    update,
    aggregate_sort_and_limit,
    insert_map,
    find_sort_skip_limit_test,
    find_one_test,
    run_map_command
  ].

init_per_suite(Config) ->
  application:ensure_all_started(mongodb),
  [{database, <<"test">>} | Config].

end_per_suite(_Config) ->
  ok.

init_per_testcase(Case, Config) ->
  Login = application:get_env(mongodb, test_auth_login, undefined),
  Password = application:get_env(mongodb, test_auth_password, undefined),
  AuthConfig =
    case {Login, Password} of
        {undefined, _} -> [];
        {_, undefined} -> [];
        _ -> [{login, erlang:atom_to_binary(Login)}, {password, erlang:atom_to_binary(Password)}]
    end,
  {ok, Connection} =
    mc_worker:start_link([{database, ?config(database, Config)},
                          {w_mode, safe}]
                         ++ AuthConfig),
  [{connection, Connection}, {collection, mc_test_utils:collection(?MODULE, Case)} | Config].

end_per_testcase(_Case, Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),
  {true, _} = mc_worker_api:delete(Connection, Collection, #{}).

%% Tests
insert_and_find(Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),

  NationalTeams = [
    #{<<"name">> => <<"Mets">>,
      <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
      <<"league">> => <<"National">>},
    #{<<"name">> => <<"Phillies">>,
      <<"home">> => #{<<"city">> => <<"Philadelphia">>, <<"state">> => <<"PA">>},
      <<"league">> => <<"National">>}
  ],
  AmericanTeams = [
    #{<<"name">> => <<"Yankees">>,
      <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
      <<"league">> => <<"American">>},
    #{<<"name">> => <<"Red Sox">>,
      <<"home">>=> #{<<"city">> => <<"Boston">>, <<"state">> => <<"MA">>},
      <<"league">> => <<"American">>}
  ],
  TeamList = NationalTeams ++ AmericanTeams,

  {{true, _}, Teams} = mc_worker_api:insert(Connection, Collection, TeamList),
  4 = mc_worker_api:count(Connection, Collection, #{}),
  {ok, TeamsCur} = mc_worker_api:find(Connection, Collection, #{}),
  TeamsFound = mc_cursor:rest(TeamsCur),
  undefined = process_info(TeamsCur),
  ?assertEqual(Teams, TeamsFound),

  {ok, NationalTeamsCur} = mc_worker_api:find(
    Connection, Collection, #{<<"league">> => <<"National">>}, #{projector => #{<<"_id">> => false}}),
  NationalTeamsFound = mc_cursor:rest(NationalTeamsCur),
  ?assertEqual(NationalTeams, NationalTeamsFound),

  2 = mc_worker_api:count(Connection, Collection, #{<<"league">> => <<"National">>}),
  TeamNames = lists:map(fun(#{<<"name">> := Name}) -> #{<<"name">> => Name} end, TeamList),
  {ok, TeamNamesCur} = mc_worker_api:find(
    Connection, Collection, #{}, #{projector => #{<<"_id">> => false, <<"name">> => true}}),
  TeamNamesFound = mc_cursor:rest(TeamNamesCur),
  ?assertEqual(TeamNames, TeamNamesFound),

  BostonTeam = mc_worker_api:find_one(Connection, Collection,
    #{<<"home">> => #{<<"city">> => <<"Boston">>, <<"state">> => <<"MA">>}}),

  #{<<"name">> := <<"Red Sox">>,
    <<"home">> := #{<<"city">> := <<"Boston">>, <<"state">> := <<"MA">>},
    <<"league">> := <<"American">>} = BostonTeam,
  Config.

find_one_test(Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),

  {{true, #{<<"n">> := 4}}, _} = mc_worker_api:insert(Connection, Collection, [
    #{<<"name">> => <<"Yankees">>,
      <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
      <<"league">> => <<"American">>},
    #{<<"name">> => <<"Mets">>,
      <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
      <<"league">> => <<"National">>},
    #{<<"name">> => <<"Phillies">>,
      <<"home">> => #{<<"city">> => <<"Philadelphia">>, <<"state">> => <<"PA">>},
      <<"league">> => <<"National">>},
    #{<<"name">> => <<"Red Sox">>,
      <<"home">>=> #{<<"city">> => <<"Boston">>, <<"state">> => <<"MA">>},
      <<"league">> => <<"American">>}
  ]),

  #{<<"name">> := <<"Yankees">>,
    <<"home">> := #{<<"city">> := <<"New York">>, <<"state">> := <<"NY">>},
    <<"league">> := <<"American">>} = mc_worker_api:find_one(Connection, Collection, #{}),
  Config.

insert_and_delete(Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),

  {{true, #{<<"n">> := 4}}, _} = mc_worker_api:insert(Connection, Collection, [
    #{<<"name">> => <<"Yankees">>,
      <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
      <<"league">> => <<"American">>},
    #{<<"name">> => <<"Mets">>,
      <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
      <<"league">> => <<"National">>},
    #{<<"name">> => <<"Phillies">>,
      <<"home">> => #{<<"city">> => <<"Philadelphia">>, <<"state">> => <<"PA">>},
      <<"league">> => <<"National">>},
    #{<<"name">> => <<"Red Sox">>,
      <<"home">>=> #{<<"city">> => <<"Boston">>, <<"state">> => <<"MA">>},
      <<"league">> => <<"American">>}
  ]),
  4 = mc_worker_api:count(Connection, Collection, #{}),

  mc_worker_api:delete_one(Connection, Collection, #{}),
  3 = mc_worker_api:count(Connection, Collection, #{}),

  mc_worker_api:delete_limit(Connection, Collection, #{}, 1),
  2 = mc_worker_api:count(Connection, Collection, #{}),

  mc_worker_api:delete(Connection, Collection, #{}),
  0 = mc_worker_api:count(Connection, Collection, #{}),

  Config.

insert_map(Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),

  Map = #{
    <<"name">> => <<"Yankees">>,
    <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
    <<"league">> => <<"American">>},
  {{true, _}, _} = mc_worker_api:insert(Connection, Collection, Map),

  Res = mc_worker_api:find_one(Connection, Collection,
    #{<<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>}}),
  #{<<"home">> := #{<<"city">> := <<"New York">>, <<"state">> := <<"NY">>},
    <<"league">> := <<"American">>, <<"name">> := <<"Yankees">>} = Res,
  Config.

search_and_query(Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),

  %insert test data
  {{true, _}, _} = mc_worker_api:insert(Connection, Collection, [
    #{<<"name">> => <<"Yankees">>,
      <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
      <<"league">> => <<"American">>},
    #{<<"name">> => <<"Mets">>,
      <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
      <<"league">> => <<"National">>},
    #{<<"name">> => <<"Phillies">>,
      <<"home">> => #{<<"city">> => <<"Philadelphia">>, <<"state">> => <<"PA">>},
      <<"league">> => <<"National">>},
    #{<<"name">> => <<"Red Sox">>,
      <<"home">> => #{<<"city">> => <<"Boston">>, <<"state">> => <<"MA">>},
      <<"league">> => <<"American">>}
  ]),
  %test selector
  {ok, Cur} = mc_worker_api:find(Connection, Collection,
    #{<<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>}}),
  Res = mc_cursor:rest(Cur),

  [Yankees1, Mets1] = Res,
  #{<<"name">> := <<"Yankees">>, <<"home">> := #{<<"city">> := <<"New York">>, <<"state">> := <<"NY">>},
    <<"league">> := <<"American">>} = Yankees1,
  #{<<"name">> := <<"Mets">>, <<"home">> := #{<<"city">> := <<"New York">>, <<"state">> := <<"NY">>},
    <<"league">> := <<"National">>} = Mets1,

  %test projector
  {ok, Cur2} = mc_worker_api:find(Connection, Collection,
    #{<<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>}},
    #{<<"name">> => true, <<"league">> => true}),
  [
    #{<<"name">> := <<"Yankees">>, <<"league">> := <<"American">>},
    #{<<"name">> := <<"Mets">>, <<"league">> := <<"National">>}
  ] = mc_cursor:rest(Cur2),
  Config.

aggregate_sort_and_limit(Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),

  %insert test data
  {{true, _}, _} = mc_worker_api:insert(Connection, Collection, [
    #{<<"key">> => <<"test">>, <<"value">> => <<"two">>, <<"tag">> => 2},
    #{<<"key">> => <<"test">>, <<"value">> => <<"one">>, <<"tag">> => 1},
    #{<<"key">> => <<"test">>, <<"value">> => <<"four">>, <<"tag">> => 4},
    #{<<"key">> => <<"another">>, <<"value">> => <<"five">>, <<"tag">> => 5},
    #{<<"key">> => <<"test">>, <<"value">> => <<"three">>, <<"tag">> => 3}
  ]),

  %test match and sort
  {true, #{<<"cursor">> := #{<<"firstBatch">> := Res}}} = mc_worker_api:command(Connection,
    {<<"aggregate">>,
     Collection,
     <<"pipeline">>,
     [{<<"$match">>, {<<"key">>, <<"test">>}}, {<<"$sort">>, {<<"tag">>, 1}}],
     <<"cursor">>,
     {}
    }),

  [
    #{<<"key">> := <<"test">>, <<"value">> := <<"one">>, <<"tag">> := 1},
    #{<<"key">> := <<"test">>, <<"value">> := <<"two">>, <<"tag">> := 2},
    #{<<"key">> := <<"test">>, <<"value">> := <<"three">>, <<"tag">> := 3},
    #{<<"key">> := <<"test">>, <<"value">> := <<"four">>, <<"tag">> := 4}
  ] = Res,

  %test match & sort with limit
  {true, #{<<"cursor">> := #{<<"firstBatch">> := Res1}}} = mc_worker_api:command(Connection,
    {<<"aggregate">>, Collection, <<"pipeline">>,
      [
        {<<"$match">>, {<<"key">>, <<"test">>}},
        {<<"$sort">>, {<<"tag">>, 1}},
        {<<"$limit">>, 1}
      ],
    <<"cursor">>, {}}),

  [
    #{<<"key">> := <<"test">>, <<"value">>:= <<"one">>, <<"tag">> := 1}
  ] = Res1,
  Config.

find_sort_skip_limit_test(Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),

  %insert test data
  {{true, #{<<"n">> := 16}}, _} = mc_worker_api:insert(Connection, Collection, [
    #{<<"key">> => <<"test1">>, <<"value">> => <<"val1">>, <<"tag">> => 1},
    #{<<"key">> => <<"test2">>, <<"value">> => <<"val2">>, <<"tag">> => 2},
    #{<<"key">> => <<"test3">>, <<"value">> => <<"val3">>, <<"tag">> => 3},
    #{<<"key">> => <<"test4">>, <<"value">> => <<"val4">>, <<"tag">> => 4},
    #{<<"key">> => <<"test5">>, <<"value">> => <<"val5">>, <<"tag">> => 5},
    #{<<"key">> => <<"test6">>, <<"value">> => <<"val6">>, <<"tag">> => 6},
    #{<<"key">> => <<"test7">>, <<"value">> => <<"val7">>, <<"tag">> => 7},
    #{<<"key">> => <<"test8">>, <<"value">> => <<"val8">>, <<"tag">> => 8},
    #{<<"key">> => <<"test9">>, <<"value">> => <<"val9">>, <<"tag">> => 9},
    #{<<"key">> => <<"testA">>, <<"value">> => <<"valA">>, <<"tag">> => 10},
    #{<<"key">> => <<"testB">>, <<"value">> => <<"valB">>, <<"tag">> => 11},
    #{<<"key">> => <<"testC">>, <<"value">> => <<"valC">>, <<"tag">> => 12},
    #{<<"key">> => <<"testD">>, <<"value">> => <<"valD">>, <<"tag">> => 13},
    #{<<"key">> => <<"testE">>, <<"value">> => <<"valE">>, <<"tag">> => 14},
    #{<<"key">> => <<"testF">>, <<"value">> => <<"valF">>, <<"tag">> => 15},
    #{<<"key">> => <<"testG">>, <<"value">> => <<"valG">>, <<"tag">> => 16}
  ]),

  Selector = #{<<"$query">> => {}, <<"$orderby">> => #{<<"tag">> => -1}},
  Args = #{batchsize => 5, skip => 10},
  {ok, C} = mc_worker_api:find(Connection, Collection, Selector, Args),

  [
    #{<<"key">> := <<"test6">>, <<"value">> := <<"val6">>, <<"tag">> := 6},
    #{<<"key">> := <<"test5">>, <<"value">> := <<"val5">>, <<"tag">> := 5},
    #{<<"key">> := <<"test4">>, <<"value">> := <<"val4">>, <<"tag">> := 4},
    #{<<"key">> := <<"test3">>, <<"value">> := <<"val3">>, <<"tag">> := 3},
    #{<<"key">> := <<"test2">>, <<"value">> := <<"val2">>, <<"tag">> := 2}
  ] = mc_cursor:next_batch(C),
  mc_cursor:close(C),

  Config.

update(Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),

  %insert test data
  {{true, _}, _} = mc_worker_api:insert(Connection, Collection,
    #{<<"_id">> => 100,
      <<"sku">> => <<"abc123">>,
      <<"quantity">> => 250,
      <<"instock">> => true,
      <<"reorder">> => false,
      <<"details">> => {<<"model">>, "14Q2", <<"make">>, "xyz"},
      <<"tags">> => ["apparel", "clothing"],
      <<"ratings">> => [#{<<"by">> => "ijk", <<"rating">> => 4}]}
  ),

  %check data inserted
  {ok, Cursor} = mc_worker_api:find(Connection, Collection, #{<<"_id">> => 100}),
  [Res] = mc_cursor:rest(Cursor),
  #{<<"_id">> := 100,
    <<"sku">> := <<"abc123">>,
    <<"quantity">> := 250,
    <<"instock">> := true,
    <<"reorder">> := false,
    <<"details">> := #{<<"model">> := "14Q2", <<"make">> := "xyz"},
    <<"tags">> := ["apparel", "clothing"],
    <<"ratings">> := [#{<<"by">> := "ijk", <<"rating">> := 4}]} = Res,

  %update existent fields
  Command = #{
    <<"quantity">> => 500,
    <<"details">> => #{<<"model">> => "14Q3"},  %with flatten_map there is no need to specify non-changeble data
    <<"tags">> => ["coats", "outerwear", "clothing"]
  },
  mc_worker_api:update(Connection, Collection, #{<<"_id">> => 100}, #{<<"$set">> => bson:flatten_map(Command)}),

  %check data updated
  {ok, Cursor1} = mc_worker_api:find(Connection, Collection, #{<<"_id">> => 100}),
  [Res1] = mc_cursor:rest(Cursor1),

  #{<<"_id">> := 100,
    <<"sku">> := <<"abc123">>,
    <<"quantity">> := 500,
    <<"instock">> := true,
    <<"reorder">> := false,
    <<"details">> := #{<<"model">> := "14Q3", <<"make">> := "xyz"},
    <<"tags">> := ["coats", "outerwear", "clothing"],
    <<"ratings">> := [#{<<"by">> := "ijk", <<"rating">> := 4}]} = Res1,

  %update non existent fields
  Command1 = {<<"$set">>, {<<"expired">>, true}},
  mc_worker_api:update(Connection, Collection, #{<<"_id">> => 100}, Command1),

  %check data updated
  {ok, Cursor2} = mc_worker_api:find(Connection, Collection, #{<<"_id">> => 100}),
  [Res2] = mc_cursor:rest(Cursor2),

  #{<<"_id">> := 100,
    <<"sku">> := <<"abc123">>,
    <<"quantity">> := 500,
    <<"instock">> := true,
    <<"reorder">> := false,
    <<"details">> := #{<<"model">> := "14Q3", <<"make">> := "xyz"},
    <<"tags">> := ["coats", "outerwear", "clothing"],
    <<"ratings">> := [#{<<"by">> := "ijk", <<"rating">> := 4}],
    <<"expired">> := true} = Res2,

  %update embedded fields
  Command2 = {<<"$set">>, {<<"details.make">>, "zzz"}},
  mc_worker_api:update(Connection, Collection, #{<<"_id">> => 100}, Command2),

  %check data updated
  {ok, Cursor3} = mc_worker_api:find(Connection, Collection, {<<"_id">>, 100}),
  [Res3] = mc_cursor:rest(Cursor3),

  #{<<"_id">> := 100,
    <<"sku">> := <<"abc123">>,
    <<"quantity">> := 500,
    <<"instock">> := true,
    <<"reorder">> := false,
    <<"details">> := #{<<"model">> := "14Q3", <<"make">> := "zzz"},
    <<"tags">> := ["coats", "outerwear", "clothing"],
    <<"ratings">> := [#{<<"by">> := "ijk", <<"rating">> := 4}],
    <<"expired">> := true} = Res3,

%update list elements
  Command3 = {<<"$set">>, {
    <<"tags.1">>, "rain gear",
    <<"ratings.0.rating">>, 2
  }},
  mc_worker_api:update(Connection, Collection, #{<<"_id">> => 100}, Command3),
  {ok, Cursor4} = mc_worker_api:find(Connection, Collection, {<<"_id">>, 100}),
  [Res4] = mc_cursor:rest(Cursor4),
  #{<<"_id">> := 100,
    <<"sku">> := <<"abc123">>,
    <<"quantity">> := 500,
    <<"instock">> := true,
    <<"reorder">> := false,
    <<"details">> := #{<<"model">> := "14Q3", <<"make">> :="zzz"},
    <<"tags">> := ["coats", "rain gear", "clothing"],
    <<"ratings">> := [#{<<"by">> := "ijk", <<"rating">> := 2}],
    <<"expired">> := true} = Res4,
  Config.


%% One should be able to specify a command as a map.
run_map_command(Config) ->
  Connection = ?config(connection, Config),
  Collection = ?config(collection, Config),
  %% When mc_worker_api:command translates the map to a  BSON document, the
  %% command name key need to be placed first.
  %%
  %% Perform a few commands so we can be relativly sure that the command key
  %% will not be placed first in at least one of them when doing maps:to_list/1
  try
      HelloCommand = #{
                       <<"hello">> => 1,
                       <<"comment">> => <<"hello command comment">>,
                       <<"saslSupportedMechs">> => <<"<db.username_not_existing>">>
                      },
      {true, #{}} = mc_worker_api:command(Connection, HelloCommand)
  catch
      _:_ ->
          %% Assume it is an old version that does not support the hello command
          IsMasterCommand = #{
                           <<"isMaster">> => 1,
                           <<"comment">> => <<"hello command comment">>,
                           <<"saslSupportedMechs">> => <<"<db.username_not_existing>">>
                          },
          {true, #{}} = mc_worker_api:command(Connection, IsMasterCommand)
  end,
  InsertCommand = #{
                    <<"insert">> => Collection,
                    <<"documents">> => [#{<<"field1">> => 1}, #{<<"field1">> => 1}],
                    <<"ordered">> => true,
                    <<"bypassDocumentValidation">> => false,
                    <<"comment">> => <<"test insert command">>
                   },
  {true, #{<<"n">>:=2}} = mc_worker_api:command(Connection, InsertCommand),
  DeleteCommand = #{
                    <<"delete">> => Collection,
                    <<"deletes">> => [
                                      #{
                                        <<"q">> => #{field1 => 1},
                                        <<"limit">> => 0
                                       }
                                     ],
                    <<"comment">> => <<"This is a comment">>,
                    <<"ordered">> => true 
                   },
  {true,#{<<"n">>:=2}} = mc_worker_api:command(Connection, DeleteCommand),
  ok.
