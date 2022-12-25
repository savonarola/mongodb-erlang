-module(mc_super_sup).
-behaviour(supervisor).

-export([start_link/0]).

-export([
	init/1,
        ensure_mc_worker_pid_info_server/0
]).

-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

-spec start_link() -> {ok, pid()}.
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, app).

%% @hidden
init(app) ->
	MongoIdServer = ?CHILD(mongo_id_server, worker),
	PIDInfoServer = ?CHILD(mc_worker_pid_info, worker),
	{ok, {{one_for_one, 1000, 3600}, [MongoIdServer, PIDInfoServer]}}.


ensure_mc_worker_pid_info_server() ->
    %% We don't want this to crash because this might fail an hot-upgrade
    try
        case ets:whereis(mc_worker_pid_info:get_mc_worker_pid_info_tab_name()) of
            undefined -> 
                gen_server:start(mc_worker_pid_info, [], []),
                ok;
            _ -> ok
        end
    catch
        _:_ -> ok
    end.
