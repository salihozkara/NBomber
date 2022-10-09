module internal NBomber.Domain.StepContext

open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

open NBomber
open NBomber.Contracts

let getClient (scnInfo: ScenarioInfo) (factory: IUntypedClientFactory option) =
    match factory with
    | Some v ->
        let index = scnInfo.ThreadNumber % v.ClientCount
        v.GetClient index

    | None -> Unchecked.defaultof<_>

let getFromData (key: string) (data: Dictionary<string,obj>) =
    let item = data[key]
    item :?> 'T

let getPreviousStepResponse (data: Dictionary<string,obj>) =
    try
        let prevStepResponse = data[Constants.StepResponseKey]
        if isNull prevStepResponse then
            Unchecked.defaultof<'T>
        else
            prevStepResponse :?> 'T
    with
    | ex -> Unchecked.defaultof<'T>

let toUntypedExecute (execute: IStepContext<'TClient,'TFeedItem> -> Task<Response>) =
    fun (untyped: IUntypedStepContext) ->
        execute(untyped :?> IStepContext<'TClient,'TFeedItem>)

type StepContext<'TClient,'TFeedItem>(logger, stepName, scenarioInfo, stopTest, stopScenario) =

    let mutable _cancelTokenSrc = Unchecked.defaultof<CancellationTokenSource>
    let mutable _client = Unchecked.defaultof<obj>
    let mutable _data = Unchecked.defaultof<Dictionary<string,obj>>
    let mutable _feedItem = Unchecked.defaultof<obj>
    let mutable _invocationNumber = 0

    interface IUntypedStepContext with
        member _.Logger = logger
        member _.StepName = stepName
        member _.ScenarioInfo = scenarioInfo
        member _.CancellationTokenSource with get() = _cancelTokenSrc and set v = _cancelTokenSrc <- v
        member _.Client with get() = _client and set v = _client <- v
        member _.Data with get() = _data and set v = _data <- v
        member _.FeedItem with get() = _feedItem and set v = _feedItem <- v
        member _.InvocationNumber with get() = _invocationNumber and set v = _invocationNumber <- v

    interface IStepContext<'TClient,'TFeedItem> with
        member _.StepName = stepName
        member _.ScenarioInfo = scenarioInfo
        member _.CancellationToken = _cancelTokenSrc.Token
        member _.Client = _client :?> 'TClient
        member _.Data = _data
        member _.FeedItem = _feedItem :?> 'TFeedItem
        member _.GetFromData(key) = getFromData key _data
        member _.GetPreviousStepResponse() = getPreviousStepResponse _data
        member _.InvocationNumber = _invocationNumber
        member _.Logger = logger
        member _.StopCurrentTest(reason) = stopTest reason
        member _.StopScenario(scenarioName, reason) = stopScenario(scenarioName, reason)
