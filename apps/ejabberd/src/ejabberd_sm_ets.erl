%%%-------------------------------------------------------------------
%%% @author Michal Ptaszek <huang.kebo@gmail.com>
%%% @copyright (C) 2011, Erlang Solutions Ltd.
%%% @doc Implementation of ets-based session manager
%%%
%%% @end
%%% Created : 17 Nov 2011 by Michal Ptaszek <huang.kebo@gmail.com>
%%%-------------------------------------------------------------------
-module(ejabberd_sm_ets).
-behavior(ejabberd_gen_sm).
-include("ejabberd.hrl").

-export([start/1,
         get_sessions/0,
         get_sessions/1,
         get_sessions/2,
         get_sessions/3,
         create_session/4,
         delete_session/4,
         cleanup/1,
         total_count/0,
         unique_count/0]).

-define(TABLE_SESSION, 'ets_sm_session').
-define(TABLE_SESSION_INDEX, 'ets_sm_session_ind').
-define(TABLE_SESSION_INDEX2, 'ets_sm_session_ind2').

-spec start(list()) -> any().
start(_Opts) ->
    ets:new(?TABLE_SESSION, [set, named_table, public,
        {keypos, #session.sid}, {read_concurrency, true}
    ]),
    ets:new(?TABLE_SESSION_INDEX, [bag, named_table, public,
        {keypos, 1}, {read_concurrency, true}
    ]), % {us, sid}
    ets:new(?TABLE_SESSION_INDEX2, [set, named_table, public,
        {keypos, 1}, {read_concurrency, true}
    ]). % {user, sid}


-spec get_sessions() -> [ejabberd_sm:ses_tuple()].
get_sessions() ->
    Sessions = ets:tab2list(?TABLE_SESSION),
    [{USR, SID, Pri, Info} ||
        #session{usr = USR, sid = SID, priority = Pri, info = Info} <- Sessions ].

-spec get_sessions(ejabberd:server()) -> [ejabberd_sm:ses_tuple()].
get_sessions(Server) ->
    Sessions = ets:tab2list(?TABLE_SESSION),
    [{USR, SID, Pri, Info} ||
        #session{
            usr = USR,
            sid = SID,
            priority = Pri,
            info = Info,
            us = {_, Server}
        } <- Sessions].

-spec get_sessions(ejabberd:user(), ejabberd:server()) -> [ejabberd_sm:session()].
get_sessions(User, Server) ->
    Sids = ets:lookup(?TABLE_SESSION_INDEX, {User, Server}),
    lists:flatten([ets:lookup(?TABLE_SESSION, Sid) || {_, Sid} <- Sids]).

-spec get_sessions(ejabberd:user(), ejabberd:server(), ejabberd:resource()
                  ) -> [ejabberd_sm:session()].
get_sessions(User, Server, Resource) ->
    Sids = ets:lookup(?TABLE_SESSION_INDEX2, {User, Server, Resource}),
    lists:flatten([ets:lookup(?TABLE_SESSION, Sid) || {_, Sid} <- Sids]).

-spec create_session(_User :: ejabberd:user(),
                     _Server :: ejabberd:server(),
                     _Resource :: ejabberd:resource(),
                     Session :: ejabberd_sm:session()) -> ok | {error, term()}.
create_session(User, Server, Resource, Session) ->
    case get_sessions(User, Server, Resource) of
        [] ->
            ets:insert(?TABLE_SESSION, Session),
            ets:insert(?TABLE_SESSION_INDEX, {{User, Server}, Session#session.sid}),
            ets:insert(?TABLE_SESSION_INDEX2, {{User, Server, Resource}, Session#session.sid});
        Sessions when is_list(Sessions) ->
            lists:foreach(fun(SessionPri) ->
		catch ets:delete(?TABLE_SESSION, SessionPri#session.sid),
                catch ets:match_delete(?TABLE_SESSION_INDEX, {{User, Server}, SessionPri#session.sid})
            end, Sessions),
            ets:insert(?TABLE_SESSION, Session),
            ets:insert(?TABLE_SESSION_INDEX, {{User, Server}, Session#session.sid}),
            ets:insert(?TABLE_SESSION_INDEX2, {{User, Server, Resource}, Session#session.sid})

    end,
    ok.

-spec delete_session(ejabberd_sm:sid(),
                     _User :: ejabberd:user(),
                     _Server :: ejabberd:server(),
                     _Resource :: ejabberd:resource()) -> ok.
delete_session(SID, _User, _Server, _Resource) ->
    catch ets:delete(?TABLE_SESSION, SID),
    catch ets:match_delete(?TABLE_SESSION_INDEX, {'_', SID}),
    catch ets:match_delete(?TABLE_SESSION_INDEX2, {'_', SID}),
    ok.

-spec cleanup(atom()) -> any().
cleanup(Node) ->
    Sessions = ets:tab2list(?TABLE_SESSION),
    ets:delete_all_objects(?TABLE_SESSION),
    ets:delete_all_objects(?TABLE_SESSION_INDEX),
    ets:delete_all_objects(?TABLE_SESSION_INDEX2),
    lists:foreach(
    fun(#session{usr = {U, S, R}, sid = SID} = Session) ->
       ejabberd_hooks:run(session_cleanup, S, [U, S, R, SID])
    end, Sessions).

-spec total_count() -> integer().
total_count() ->
    ets:info(?TABLE_SESSION, size).

-spec unique_count() -> integer().
unique_count() ->
    ets:info(?TABLE_SESSION, size).
