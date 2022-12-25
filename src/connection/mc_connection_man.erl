%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc  mongo client connection manager
%%% processes request and response to|from database
%%% @end
%%% Created : 28. май 2014 18:37
%%%-------------------------------------------------------------------
-module(mc_connection_man).
-author("tihon").

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").

-define(NOT_MASTER_ERROR, 13435).
-define(UNAUTHORIZED_ERROR(C), C =:= 10057; C =:= 16550).

%% API
-export([request_worker/2, process_reply/2]).
-export([read/2, read_one/2, read_one_sync/4]).
-export([op_msg/2, op_msg_sync/4, op_msg_read_one/2, op_msg_raw_result/2, request_raw_no_parse/4]).

-spec read(pid() | atom(), query()) -> [] | {ok, pid()}.
read(Connection, Request = #'query'{collection = Collection, batchsize = BatchSize}) ->
    read(Connection, Request, Collection, BatchSize);
read(Connection, #'op_msg_command'{command_doc = ([{_, Collection} | _ ] = Fields)} = Request)  ->
    BatchSize = case lists:keyfind(<<"batchSize">>, 1, Fields) of
                    {_, Size} -> Size;
                    false -> 101
                end,
    read(Connection, Request, Collection, BatchSize).

read(Connection, Request, Collection, BatchSize) ->
    case request_worker(Connection, Request) of
        {_, []} ->
            [];
        {Cursor, Batch} ->
            mc_cursor:start(Connection, Collection, Cursor, BatchSize, Batch);
        X ->
            erlang:error({error_unexpected_response, X})
    end.

-spec read_one(pid() | atom(), query()) -> undefined | map().
read_one(Connection, Request) ->
  {0, Docs} = request_worker(Connection, Request#'query'{batchsize = -1}),
  case Docs of
    [] -> undefined;
    [Doc | _] -> Doc
  end.

op_msg_raw_result(Connection, OpMsg) ->
    Timeout = mc_utils:get_timeout(),
    FromServer = gen_server:call(Connection, OpMsg, Timeout),
    case FromServer of
        #op_msg_response{response_doc =
                         (#{<<"ok">> := 1.0} = Res)} ->
            Res;
        _ ->
            erlang:error({error, FromServer})
    end.

op_msg(Connection, OpMsg) ->
  Doc = request_worker(Connection, OpMsg),
  process_reply(Doc, OpMsg).

op_msg_read_one(Connection, OpMsg) ->
  Timeout = mc_utils:get_timeout(),
  Response = gen_server:call(Connection, OpMsg, Timeout),
  case Response of
      #op_msg_response{response_doc =
                       #{<<"ok">> := 1.0,
                         <<"cursor">>:=
                         #{<<"firstBatch">>:=[Doc],
                           <<"id">>:=0}
                        }} ->
          Doc;
      #op_msg_response{response_doc =
                       #{<<"ok">> := 1.0}} ->
          undefined;
      #op_msg_response{response_doc = Doc} ->
          erlang:error({error, Doc});
      _ ->
          erlang:error({error_unexpected_response, Response})
  end.

-spec request_worker(pid(), mongo_protocol:message()) -> ok | {non_neg_integer(), [map()]} | map().
request_worker(Connection, Request) ->  %request to worker
  Timeout = mc_utils:get_timeout(),
  FromServer = gen_server:call(Connection, Request, Timeout),
  reply(FromServer).

process_reply(Doc = #{<<"ok">> := N}, _) when is_number(N) ->   %command succeed | failed
  {N == 1, maps:remove(<<"ok">>, Doc)};
process_reply(Doc, Command) -> %unknown result
  erlang:error({bad_command, Doc}, [Command]).

read_one_sync(Socket, Database, Request, SetOpts) ->
  {0, Docs} = request_raw(Socket, Database, Request#'query'{batchsize = -1}, SetOpts),
  case Docs of
    [] -> #{};
    [Doc | _] -> Doc
  end.

op_msg_sync(Socket, Database, Request, SetOpts) ->
  request_raw(Socket, Database, Request, SetOpts).

%% @private
reply(ok) -> ok;
reply(#reply{cursornotfound = false, queryerror = false} = Reply) ->
  {Reply#reply.cursorid, Reply#reply.documents};
reply(#reply{cursornotfound = false, queryerror = true} = Reply) ->
  [Doc | _] = Reply#reply.documents,
  process_error(maps:get(<<"code">>, Doc), Doc);
reply(#reply{cursornotfound = true, queryerror = false} = Reply) ->
  erlang:error({bad_cursor, Reply#reply.cursorid});
reply({error, Error}) ->
  process_error(error, Error);
reply(#op_msg_response{response_doc = (#{<<"cursor">>:=#{<<"firstBatch">>:=Batch,<<"id">>:=Id}} = Doc)}) when map_get(<<"ok">>, Doc) == 1 -> 
  {Id, Batch};
reply(#op_msg_response{response_doc = Document}) when map_get(<<"ok">>, Document) == 1 ->
  Document;
reply(Resp) ->
  erlang:error({error_cannot_parse_response, Resp}).

%% @private
-spec process_error(atom() | integer(), term()) -> no_return().
process_error(?NOT_MASTER_ERROR, _) ->
  erlang:error(not_master);
process_error(Code, _) when ?UNAUTHORIZED_ERROR(Code) ->
  erlang:error(unauthorized);
process_error(_, Doc) ->
  erlang:error({bad_query, Doc}).

%% @private
-spec request_raw(any(), mc_worker_api:database(), mongo_protocol:message(), module()) ->
  ok | {non_neg_integer(), [map()]}.
request_raw(Socket, Database, Request, NetModule) ->
  Timeout = mc_utils:get_timeout(),
  ok = set_opts(Socket, NetModule, false),
  {ok, _, _} = mc_worker_logic:make_request(Socket, NetModule, Database, Request),
  Responses = recv_all(Socket, Timeout, NetModule),
  ok = set_opts(Socket, NetModule, true),
  {_Id, Reply} = hd(Responses),
  reply(Reply).

%% @private
request_raw_no_parse(Socket, Database, Request, NetModule) ->
  Timeout = mc_utils:get_timeout(),
  ok = set_opts(Socket, NetModule, false),
  {ok, _, _} = mc_worker_logic:make_request(Socket, NetModule, Database, Request),
  Result = recv_all(Socket, Timeout, NetModule),
  ok = set_opts(Socket, NetModule, true),
  Result.

%% @private
set_opts(Socket, ssl, Value) ->
  ssl:setopts(Socket, [{active, Value}]);
set_opts(Socket, gen_tcp, Value) ->
  inet:setopts(Socket, [{active, Value}]).

%% @private
recv_all(Socket, Timeout, NetModule) ->
  recv_all(Socket, Timeout, NetModule, <<>>).
recv_all(Socket, Timeout, NetModule, Rest) ->
  {ok, Packet} = NetModule:recv(Socket, 0, Timeout),
  case mc_worker_logic:decode_responses(<<Rest/binary, Packet/binary>>) of
    {[], Unfinished} -> recv_all(Socket, Timeout, NetModule, Unfinished);
    {Responses, _} -> Responses
  end.
