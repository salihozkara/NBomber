module internal NBomber.Domain.RunningStep

open System
open System.Collections.Generic
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

open Serilog

open NBomber
open NBomber.Contracts
open NBomber.Contracts.Internal
open NBomber.Domain.DomainTypes
open NBomber.Domain.Stats.ScenarioStatsActor

type RunningScenarioContext = {
    Logger: ILogger
    Scenario: Scenario
    ScenarioCancellationToken: CancellationTokenSource
    ScenarioTimer: Stopwatch
    ScenarioOperation: ScenarioOperation
    ScenarioStatsActor: ScenarioStatsActor
    ExecStopCommand: StopCommand -> unit
    MaxFailCount: int
}

type RunningStepContext = {
    ScenarioExecContext: RunningScenarioContext
    ScenarioInfo: ScenarioInfo
    Data: Dictionary<string,obj>
}

let create (stCtx: RunningStepContext) (stepIndex: int) (step: Step) =

    let args = {
        Logger = stCtx.ScenarioExecContext.Logger
        ScenarioInfo = stCtx.ScenarioInfo
        StopTest = fun reason -> StopTest(reason) |> stCtx.ScenarioExecContext.ExecStopCommand
        StopScenario = fun (scnName,reason) -> StopScenario(scnName, reason) |> stCtx.ScenarioExecContext.ExecStopCommand
    }

    { StepIndex = stepIndex; Value = step; Context = step.CreateEmptyStepContext args }

let updateContext (step: RunningStep) (data: Dictionary<string,obj>) (cancelToken: CancellationTokenSource) =
    let st = step.Value
    let context = step.Context

    let feedItem =
        match st.Feed with
        | Some feed -> feed.GetNextItem(context.ScenarioInfo, data)
        | None      -> Unchecked.defaultof<_>

    context.CancellationTokenSource <- cancelToken
    context.InvocationNumber <- context.InvocationNumber + 1
    context.Data <- data
    context.FeedItem <- feedItem
    // context.Client should be set as the last field because init order matter here
    context.Client <- StepContext.getClient context.ScenarioInfo st.ClientFactory
    step

let measureExec (step: RunningStep) (globalTimer: Stopwatch) = backgroundTask {
    let startTime = globalTimer.Elapsed.TotalMilliseconds
    try
        let responseTask = step.Value.Execute(step.Context)

        // for pause we skip timeout logic
        if step.Value.IsPause then
            let! pause = responseTask
            return { StepIndex = step.StepIndex; ClientResponse = pause; EndTimeMs = 0.0; LatencyMs = 0.0 }
        else
            let! finishedTask = Task.WhenAny(responseTask, Task.Delay step.Value.Timeout)
            let endTime = globalTimer.Elapsed.TotalMilliseconds
            let latency = endTime - startTime

            if finishedTask.Id = responseTask.Id then
                return { StepIndex = step.StepIndex; ClientResponse = responseTask.Result; EndTimeMs = endTime; LatencyMs = latency }
            else
                step.Context.CancellationTokenSource.Cancel()
                let resp = Response.fail(statusCode = Constants.TimeoutStatusCode, error = $"step timeout: {step.Value.Timeout.TotalMilliseconds} ms")
                return { StepIndex = step.StepIndex; ClientResponse = resp; EndTimeMs = endTime; LatencyMs = latency }
    with
    | :? TaskCanceledException
    | :? OperationCanceledException ->
        let endTime = globalTimer.Elapsed.TotalMilliseconds
        let latency = endTime - startTime
        let resp = Response.fail(statusCode = Constants.TimeoutStatusCode, error = "step timeout")
        return { StepIndex = step.StepIndex; ClientResponse = resp; EndTimeMs = endTime; LatencyMs = latency }
    | ex ->
        let endTime = globalTimer.Elapsed.TotalMilliseconds
        let latency = endTime - startTime
        let resp = Response.fail(statusCode = Constants.StepUnhandledErrorCode, error = $"step unhandled exception: {ex.Message}")
        return { StepIndex = step.StepIndex; ClientResponse = resp; EndTimeMs = endTime; LatencyMs = latency }
}

let execStep (stCtx: RunningStepContext) (step: RunningStep) = backgroundTask {

    let! response = measureExec step stCtx.ScenarioExecContext.ScenarioTimer
    let payload = response.ClientResponse.Payload

    if not step.Value.DoNotTrack then
        stCtx.ScenarioExecContext.ScenarioStatsActor.Publish(AddResponse response)

        if response.ClientResponse.IsError then
            stCtx.ScenarioExecContext.Logger.Fatal($"Step '{step.Value.StepName}' from Scenario: '{stCtx.ScenarioInfo.ScenarioName}' has failed. Error: {response.ClientResponse.Message}")
        else
            stCtx.Data[Constants.StepResponseKey] <- payload

        return response.ClientResponse

    elif step.Value.IsPause then
        return response.ClientResponse

    else
        if response.ClientResponse.IsError then
            stCtx.ScenarioExecContext.Logger.Fatal($"Step '{step.Value.StepName}' from Scenario: '{stCtx.ScenarioInfo.ScenarioName}' has failed. Error: {response.ClientResponse.Message}")
        else
            stCtx.Data[Constants.StepResponseKey] <- payload

        return response.ClientResponse
}

let execCustomExec (stCtx: RunningStepContext) (steps: RunningStep[]) (stepInterception: IStepInterceptionContext voption -> string voption) = backgroundTask {
    let mutable shouldWork = true
    let mutable execContext = ValueNone

    while shouldWork
          && not stCtx.ScenarioExecContext.ScenarioCancellationToken.IsCancellationRequested
          && stCtx.ScenarioInfo.ScenarioDuration.TotalMilliseconds > (stCtx.ScenarioExecContext.ScenarioTimer.Elapsed.TotalMilliseconds + Constants.SchedulerTimerDriftMs) do

        let nextStep = stepInterception execContext
        match nextStep with
        | ValueSome stepName ->
            let stepIndex = stCtx.ScenarioExecContext.Scenario.StepOrderIndex[stepName]
            use cancelToken = new CancellationTokenSource()
            let step = updateContext steps[stepIndex] stCtx.Data cancelToken
            let! response = execStep stCtx step

            execContext <- ValueSome {
                new IStepInterceptionContext with
                    member _.PrevStepContext = step.Context
                    member _.PrevStepResponse = response
            }

        | ValueNone -> shouldWork <- false
}

let execRegularExec (stCtx: RunningStepContext) (steps: RunningStep[]) (stepsOrder: int[]) = backgroundTask {
    let mutable shouldWork = true
    for stepIndex in stepsOrder do

        if shouldWork
           && not stCtx.ScenarioExecContext.ScenarioCancellationToken.IsCancellationRequested
           && stCtx.ScenarioInfo.ScenarioDuration.TotalMilliseconds > (stCtx.ScenarioExecContext.ScenarioTimer.Elapsed.TotalMilliseconds + Constants.SchedulerTimerDriftMs) then

            use cancelToken = new CancellationTokenSource()
            let step = updateContext steps[stepIndex] stCtx.Data cancelToken
            let! response = execStep stCtx step

            if response.IsError then
                shouldWork <- false
}

let execSteps (stCtx: RunningStepContext) (steps: RunningStep[]) (stepsOrder: int[]) =
    match stCtx.ScenarioExecContext.Scenario.StepInterception with
    | Some stepInterception -> execCustomExec stCtx steps stepInterception
    | None                  -> execRegularExec stCtx steps stepsOrder
