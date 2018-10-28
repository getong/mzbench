-module(mzb_worker_runner).

-export([run_worker_script/5]).

-include_lib("mzbench_language/include/mzbl_types.hrl").

-spec run_worker_script([script_expr()], worker_env() , module(), Pool :: pid(), PoolName ::string())
    -> ok.
run_worker_script(Script, Env, Worker, PoolPid, PoolName) ->
    _ = random:seed(now()),
    ok = mzb_metrics:notify(mzb_string:format("workers.~s.started", [PoolName]), 1),
    Res = eval_script(Script, Env, Worker),
    case Res of
        {ok, _} -> ok;
        _ -> mzb_metrics:notify(mzb_string:format("workers.~s.failed", [PoolName]), 1)
    end,
    mzb_metrics:notify(mzb_string:format("workers.~s.ended", [PoolName]), 1),
    PoolPid ! {worker_result, self(), Res},
    ok.

eval_script(Script, Env, Worker) ->
    %% we don't want to call terminate if init crashes
    %% we also don't want to call terminate if another terminate crashes
    case catch_init(Worker) of
        {ok, InitState} ->
            {Res, State} = catch_eval(Script, InitState, Env, Worker),
            catch_terminate(Res, Worker, State);
        {exception, Spec} ->
            {exception, node(), Spec, undefined}
    end.

catch_eval(Script, State, Env, {Provider, _}) ->
    try
        {Result, ResState} = mzbl_interpreter:eval(Script, State, Env, Provider),
        {{ok, Result}, ResState}
    catch
        ?EXCEPTION(error, {mzbl_interpreter_runtime_error, {{Error, Reason}, ErrorState}}, Stacktrace) ->
            ST = ?GET_STACK(Stacktrace),
            {{exception, {Error, Reason, ST}}, ErrorState};
        ?EXCEPTION(C, E, Stacktrace) ->
            ST = ?GET_STACK(Stacktrace),
            {{exception, {C, E, ST}}, unknown}
    end.

catch_init({Provider, Worker}) ->
    try Provider:init(Worker) of
        InitialState -> {ok, InitialState}
    catch
        ?EXCEPTION(C, E, Stacktrace) -> {exception, {C, E, ?GET_STACK(Stacktrace)}}
    end.

catch_terminate({ok, Res}, {WorkerProvider, _}, WorkerState) ->
    try
        WorkerProvider:terminate(Res, WorkerState),
        {ok, Res}
    catch
        ?EXCEPTION(Class, Error, Stacktrace) ->
            {exception, node(), {Class, Error, ?GET_STACK(Stacktrace)}, WorkerState}
    end;
catch_terminate({exception, Spec}, _, unknown) ->
    %% do not call terminate in this case because worker provider
    %% needs it's state to call worker's terminate function
    {exception, node(), Spec, undefined};
catch_terminate({exception, Spec}, {WorkerProvider, _}, WorkerState) ->
    try
        WorkerProvider:terminate(Spec, WorkerState),
        {exception, node(), Spec, WorkerState}
    catch
        ?EXCEPTION(Class, Error, Stacktrace) ->
            {exception, node(), {Class, Error, ?GET_STACK(Stacktrace)}, unknown}
    end.
