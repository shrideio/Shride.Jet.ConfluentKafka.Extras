namespace Shride.Jet.ConfluentKafka.Extras

open System
open Jet.ConfluentKafka.FSharp
open System.Collections.Generic
open Microsoft.Extensions.Configuration


[<RequireQualifiedAccess>]
module private Result =

    let apply fR xR = 
        match fR, xR with
        | Ok f, Ok x -> Ok (f x)
        | Error errs1, Ok _ -> Error errs1
        | Ok _, Error errs2 -> Error errs2
        | Error errs1, Error errs2 -> Error (errs1 @ errs2)

    let traverseA xRn = 
        let (<*>) = apply
        let (<!>) = Result.map
        let cons head tail = head::tail
        let consR headR tailR = cons <!> headR <*> tailR
        let zero = Ok [] 
        List.foldBack consR xRn zero

    let trial f x =
        try 
            Ok <| f x
        with 
            ex -> Error ex


type private ParamResult<'a> = Result<'a, string list>


type Parameter<'a> =
    | Required
    | Value of 'a
    | ValueFunc of (unit -> 'a)


type private _RequiredConsumerProperty =
    | ClientId of Parameter<string>
    | Broker of Parameter<Uri>
    | Topics of Parameter<string seq>
    | GroupId of Parameter<string>


[<RequireQualifiedAccess>]
module RequiredConsumerProperty =

    type ClientId = ClientId of Parameter<string>
    type Broker = Broker of Parameter<Uri> 
    type Topics = Topics of Parameter<string seq>
    type GroupId = GroupId of Parameter<string>


type OptionalConsumerProperty =
    | AutoOffsetReset of Parameter<Confluent.Kafka.AutoOffsetReset>
    | FetchMaxBytes of Parameter<int>
    | FetchMinBytes of Parameter<int>
    | StatisticsInterval of Parameter<TimeSpan>
    | OffsetCommitInterval of Parameter<TimeSpan>
    | MaxBatchSize of Parameter<int>
    | MaxBatchDelay of Parameter<TimeSpan>
    | MinInFlightBytes of Parameter<int64>
    | MaxInFlightBytes of Parameter<int64>


type CustomConsumerProperty = CustomConsumerProperty of name : string * parameter : Parameter<string>


type internal ConsumerParametersBag = 
    { ClientId : ParamResult<string>
      Broker : ParamResult<Uri>
      Topics : ParamResult<string seq>
      GroupId : ParamResult<string>
      AutoOffsetReset : ParamResult<Option<Confluent.Kafka.AutoOffsetReset>>
      FetchMaxBytes : ParamResult<Option<int>>
      FetchMinBytes :  ParamResult<Option<int>>
      StatisticsInterval : ParamResult<Option<TimeSpan>>
      OffsetCommitInterval : ParamResult<Option<TimeSpan>>
      MaxBatchSize : ParamResult<Option<int>> 
      MaxBatchDelay : ParamResult<Option<TimeSpan>> 
      MinInFlightBytes : ParamResult<Option<int64>> 
      MaxInFlightBytes : ParamResult<Option<int64>>
      Custom : KeyValuePair<string, ParamResult<string>> list }


type private ConsumerProperty =
    | RequiredProperty of _RequiredConsumerProperty
    | OptionalProperty of OptionalConsumerProperty
    | CustomProperty of CustomConsumerProperty


[<RequireQualifiedAccess>]
module private ConsumerProperty =

    let [<Literal>] ClientId = "client.id"
    let [<Literal>] BootstrapServers = "bootstrap.servers"
    let [<Literal>] GroupId = "group.id"
    let [<Literal>] AutoOffsetReset = "auto.offset.reset"
    let [<Literal>] FetchMaxBytes = "fetch.max.bytes"
    let [<Literal>] FetchMinBytes = "fetch.min.bytes"
    let [<Literal>] StatisticsInterval = "statistics.interval.ms"
    let [<Literal>] OffsetCommitInterval = "auto.commit.interval.ms"
    let [<Literal>] JetTopics = "jet.topics"
    let [<Literal>] JetMaxBatchSize = "jet.max.batch.size"
    let [<Literal>] JetMaxBatchDelay = "jet.max.batch.delay"
    let [<Literal>] JetMinInFlightBytes = "jet.mix.inflight.bytes"
    let [<Literal>] JetMaxInFlightBytes = "jet.max.inflight.bytes"


    let keyf (px : string option) k =
        match px with
        | Some px -> sprintf "kafka:consumer:%s:%s" px k
        | _ -> sprintf "kafka:consumer:%s" k


    let toKey (px : string option) (p: ConsumerProperty) =
        match p with 
        | (RequiredProperty rp) ->
            match rp with
            | _RequiredConsumerProperty.ClientId _ -> ClientId
            | _RequiredConsumerProperty.Broker _ -> BootstrapServers
            | _RequiredConsumerProperty.Topics _ -> JetTopics
            | _RequiredConsumerProperty.GroupId _ -> GroupId
        | (OptionalProperty op) ->
            match op with
            | OptionalConsumerProperty.AutoOffsetReset _ -> AutoOffsetReset
            | OptionalConsumerProperty.FetchMaxBytes _ -> FetchMaxBytes
            | OptionalConsumerProperty.FetchMinBytes _ -> FetchMinBytes
            | OptionalConsumerProperty.StatisticsInterval _ -> StatisticsInterval
            | OptionalConsumerProperty.OffsetCommitInterval _ -> OffsetCommitInterval
            | OptionalConsumerProperty.MaxBatchSize _ -> JetMaxBatchSize
            | OptionalConsumerProperty.MaxBatchDelay _ -> JetMaxBatchDelay
            | OptionalConsumerProperty.MinInFlightBytes _ -> JetMinInFlightBytes
            | OptionalConsumerProperty.MaxInFlightBytes _ -> JetMaxInFlightBytes
        | (CustomProperty(CustomConsumerProperty(name, _))) -> name
        |> keyf px


[<RequireQualifiedAccess>]
module private CustomerProprtyInterpreter =


    let inline private tryGetObj<'a when 'a : null> (cfg : IConfiguration) px key =
        key |> (ConsumerProperty.keyf px >> cfg.GetValue<'a> >> Option.ofObj)


    let inline private tryGetVal<'a when 'a : struct and 'a :> ValueType and 'a : (new : unit -> 'a)> (cfg : IConfiguration) px key =
        key |> (ConsumerProperty.keyf px >> cfg.GetValue<Nullable<'a>> >> Option.ofNullable)


    let private tryConvert (px : string option) (cfg : IConfiguration) tryGet map prop =
        (px, prop)
        ||> tryGet cfg
        |> function
           | Some x -> map x
           | None -> Ok None


    let private interpretParam px key param (v : _ option) =
        match v, param with
        | None, Required -> Error [ ConsumerProperty.keyf px key ]
        | None, Value x -> Ok x
        | None, ValueFunc f -> Ok <| f()
        | Some x, _ -> Ok x


    let private interpretRequired px (cfg : IConfiguration) (rp : _RequiredConsumerProperty) (bag : ConsumerParametersBag) =

        let strToUri str = 
            str 
            |> Result.trial (Uri >> Some)
            |> Result.mapError (fun exn -> [ 
                    sprintf "Failed to parse %s, error: %s" 
                            (ConsumerProperty.keyf px ConsumerProperty.BootstrapServers) 
                            exn.Message ])

        let strToTopics (str : string) =
            str.Split [| ';' |]
            |> (Seq.map (fun s -> s.Trim()) >> Seq.indexed >> List.ofSeq)
            |> function 
               | [] -> Ok None 
               | xn -> 
                    xn
                    |> List.map (fun (i, s) -> 
                        match String.IsNullOrEmpty(s) with
                        | true -> Error [ sprintf "Invalid string value index:%i." i ]
                        | false -> Ok s)
                    |> (Result.traverseA >> Result.map (Seq.ofList >> Some))

        match rp with 
        | ClientId rep ->
            let clientId = ConsumerProperty.ClientId |> tryGetObj<string> cfg px
            { bag with ClientId = clientId |> interpretParam px ConsumerProperty.ClientId rep }
        | Broker rep ->
            let broker = ConsumerProperty.BootstrapServers|> tryConvert px cfg tryGetObj<string> strToUri
            { bag with Broker = broker |> Result.bind (interpretParam px ConsumerProperty.BootstrapServers rep) }
        | Topics rep ->
            let topics = ConsumerProperty.JetTopics |> tryConvert px cfg tryGetObj<string> strToTopics
            { bag with Topics = topics |> Result.bind (interpretParam px ConsumerProperty.JetTopics rep) }
        | GroupId rep ->
            let groupId = ConsumerProperty.GroupId |>  tryGetObj<string> cfg px
            { bag with GroupId = groupId |> interpretParam px ConsumerProperty.GroupId rep }


    let private interpretOptionalParam px key param (v : _ option) = 
        v |> (interpretParam px key param >> Result.map Some)


    let private interpretOptional px (cfg : IConfiguration) (op : OptionalConsumerProperty) (bag : ConsumerParametersBag)  =

        let strToAutoOffsetReset str =
            str
            |> Enum.TryParse<Confluent.Kafka.AutoOffsetReset>
            |> function
               | true, v -> Ok <| Some v
               | false, _ -> Error [ sprintf "Failed to parse %s" (ConsumerProperty.keyf px ConsumerProperty.AutoOffsetReset) ]

        let msToTimestamp prop ms = 
            ms
            |> (float >> Result.trial TimeSpan.FromMilliseconds >> Result.map Some)
            |> Result.mapError (fun exn -> [ sprintf "Failed to parse %s, error: %s" (ConsumerProperty.keyf px prop) exn.Message ])

        match op with
        | AutoOffsetReset p ->
            let autoOffsetReset = ConsumerProperty.AutoOffsetReset |> tryConvert px cfg tryGetObj<string> strToAutoOffsetReset
            { bag with AutoOffsetReset = autoOffsetReset |> Result.bind (interpretOptionalParam px ConsumerProperty.AutoOffsetReset p) }
        | FetchMaxBytes p ->
            let fetchMaxBytes = tryGetVal<int> cfg px ConsumerProperty.FetchMaxBytes
            { bag with FetchMaxBytes = fetchMaxBytes |> interpretOptionalParam px ConsumerProperty.FetchMaxBytes p }
        | FetchMinBytes p ->
            let fetchMinBytes = tryGetVal<int> cfg px ConsumerProperty.FetchMinBytes
            { bag with FetchMinBytes = fetchMinBytes |> interpretOptionalParam px ConsumerProperty.FetchMinBytes p }
        | StatisticsInterval p ->
            let msToStatisticsInterval = msToTimestamp ConsumerProperty.StatisticsInterval
            let statisticsInterval = ConsumerProperty.StatisticsInterval |> tryConvert px cfg tryGetVal<int> msToStatisticsInterval
            { bag with StatisticsInterval = statisticsInterval |> Result.bind (interpretOptionalParam px ConsumerProperty.StatisticsInterval p) }
        | OffsetCommitInterval p ->
            let msToOffsetCommitInterval = msToTimestamp ConsumerProperty.OffsetCommitInterval
            let offsetCommitInterval = ConsumerProperty.OffsetCommitInterval |> tryConvert px cfg tryGetVal<int> msToOffsetCommitInterval
            { bag with OffsetCommitInterval = offsetCommitInterval |> Result.bind (interpretOptionalParam px ConsumerProperty.OffsetCommitInterval p) }
        | MaxBatchSize p ->
            let maxBatchSize = tryGetVal<int> cfg px ConsumerProperty.JetMaxBatchSize
            { bag with MaxBatchSize = maxBatchSize |> interpretOptionalParam px ConsumerProperty.JetMaxBatchSize p }
        | MaxBatchDelay p ->
            let msToMaxBatchDelay = msToTimestamp ConsumerProperty.JetMaxBatchDelay
            let maxBatchDelay = ConsumerProperty.JetMaxBatchDelay|> tryConvert px cfg tryGetVal<int> msToMaxBatchDelay
            { bag with MaxBatchDelay = maxBatchDelay |> Result.bind (interpretOptionalParam px ConsumerProperty.JetMaxBatchDelay p) }
        | MaxInFlightBytes p ->
            let maxInFlightBytes = tryGetVal<int64> cfg px ConsumerProperty.JetMaxInFlightBytes
            { bag with MaxInFlightBytes = maxInFlightBytes |> interpretOptionalParam px ConsumerProperty.JetMaxInFlightBytes p }
        | MinInFlightBytes p ->
            let minInFlightBytes = tryGetVal<int64> cfg px ConsumerProperty.JetMinInFlightBytes
            { bag with MinInFlightBytes = minInFlightBytes |> interpretOptionalParam px ConsumerProperty.JetMinInFlightBytes p }


    let private interpretCustom px (cfg : IConfiguration) (CustomConsumerProperty(prop, param)) (bag : ConsumerParametersBag) =
        let paramVal =
            prop
            |> (Option.ofObj >> Option.map (fun p -> p.Trim()))
            |> (Option.map (fun p -> p |> (not << String.IsNullOrEmpty), p) >> Option.defaultValue (false, null))
            |> function
               | true, p -> Ok p
               | false, _ -> Error [ "Proporty must be a non empty valid string value." ]
            |> Result.bind (fun p ->
                bag.Custom
                |> List.tryFind (fun x -> x.Key = p)
                |> function
                   | Some _ -> Error [ sprintf "Custom property '%s' is already set." p ]
                   | None -> Ok p)
            |> Result.bind (fun p ->
                let pv = p |> tryGetObj<string> cfg px 
                pv |> interpretParam px prop param)
        let cp = KeyValuePair(key = prop, value = paramVal)
        { bag with Custom = cp :: bag.Custom }


    let interpret 
        (config : IConfiguration) 
        (prefix : string option) 
        (parameters : ConsumerProperty list)
        : ConsumerParametersBag =

        let paramsBag =
            { ClientId = Error <| [ ConsumerProperty.keyf prefix ConsumerProperty.ClientId ]
              Broker = Error <| [ ConsumerProperty.keyf prefix ConsumerProperty.BootstrapServers ]
              Topics = Error <| [ ConsumerProperty.keyf prefix ConsumerProperty.JetTopics ]
              GroupId = Error <| [ ConsumerProperty.keyf prefix ConsumerProperty.GroupId ]
              AutoOffsetReset = Ok None
              FetchMaxBytes = Ok None
              FetchMinBytes = Ok None
              StatisticsInterval = Ok None
              OffsetCommitInterval = Ok None
              MaxBatchSize = Ok None
              MaxBatchDelay = Ok None
              MinInFlightBytes = Ok None
              MaxInFlightBytes = Ok None
              Custom = List.empty }
              : ConsumerParametersBag

        let transformParamsBag bag = function
            | RequiredProperty rp -> interpretRequired prefix config rp bag
            | OptionalProperty op -> interpretOptional prefix config op bag
            | CustomProperty cp -> interpretCustom prefix config cp bag
        
        let paramsBag = 
            parameters 
            |> List.fold transformParamsBag paramsBag

        paramsBag


type KafkaConsumerConfigBuilder = private KafkaConsumerConfigBuilder of parameters : ConsumerProperty list


[<RequireQualifiedAccess>]
module KafkaConsumerConfigBuilder =


    let init 
        (RequiredConsumerProperty.ClientId clientId)
        (RequiredConsumerProperty.Broker broker)
        (RequiredConsumerProperty.GroupId groupId)
        (RequiredConsumerProperty.Topics topics) =
        let pn =
            [ _RequiredConsumerProperty.ClientId(clientId)
              _RequiredConsumerProperty.Broker(broker) 
              _RequiredConsumerProperty.GroupId(groupId) 
              _RequiredConsumerProperty.Topics(topics) ]
            |> List.map ConsumerProperty.RequiredProperty
        pn |> KafkaConsumerConfigBuilder


    let optional (KafkaConsumerConfigBuilder pn) (op : OptionalConsumerProperty) =
        pn
        |> List.choose (function | ConsumerProperty.OptionalProperty p -> Some p | _ -> None)
        |> List.tryFind (fun p -> p.GetType() = op.GetType())
        |> (function | Some p -> Result.Error [ sprintf "%O has already been set." p ] | None -> Ok pn)
        |> Result.map (fun pn -> (ConsumerProperty.OptionalProperty op) :: pn |> KafkaConsumerConfigBuilder)


    let custom (KafkaConsumerConfigBuilder pn) (cp : CustomConsumerProperty) =
        pn
        |> List.choose (function | ConsumerProperty.CustomProperty p -> Some p | _ -> None)
        |> List.tryFind (fun ((CustomConsumerProperty (x, _))) -> let (CustomConsumerProperty (p, _)) = cp in x = p)
        |> (function | Some p -> Result.Error [ sprintf "%O has already been set." p ] | None -> Ok pn)
        |> Result.map (fun pn -> (ConsumerProperty.CustomProperty cp) :: pn |> KafkaConsumerConfigBuilder)


    let private esnureParameters 
        (prefix : string option) 
        (config : IConfiguration) 
        (KafkaConsumerConfigBuilder parameters) =
        
        let section = ConsumerProperty.keyf prefix String.Empty
        
        let given = 
            config.AsEnumerable() 
            |> Seq.map(fun (KeyValue(k, _)) -> k)
            |> Seq.filter(fun k -> k.StartsWith(section))
            |> List.ofSeq
        let expected =
            parameters |> List.map (ConsumerProperty.toKey prefix)
        let unexpected =
            given |> List.except expected

        match unexpected with
        | [] -> Ok <| parameters
        | _ -> Error (unexpected |> List.map (sprintf "Unexpected parameter: '%s'."))


    let build 
        (prefix : string option) 
        (config : IConfiguration) 
        (parameters: KafkaConsumerConfigBuilder) =


        let build parameters = 
            
            let (<!>) = Result.map
            let (<*>) = Result.apply

            let paramsBag =
                parameters |> CustomerProprtyInterpreter.interpret config prefix

            let { ClientId = clientId
                  Broker = broker 
                  Topics = topics 
                  GroupId = groupId 
                  AutoOffsetReset = autoOffsetReset 
                  FetchMaxBytes = fetchMaxBytes 
                  FetchMinBytes = fetchMinBytes 
                  StatisticsInterval = statisticsInterval 
                  OffsetCommitInterval = offsetCommitInterval 
                  MaxBatchSize = maxBatchSize 
                  MaxBatchDelay = maxBatchDelay 
                  MinInFlightBytes = minInFlightBytes 
                  MaxInFlightBytes = maxInFlightBytes 
                  Custom = custom } = paramsBag

            let custom = 
                custom
                |> List.map (function 
                    | KeyValue (k, Ok v) -> Ok <| KeyValuePair(key = k, value = v)
                    | KeyValue (_, Error ern) -> Error ern)
                |> (Result.traverseA >> Result.map Some)

            let create 
                (clientId : string)
                (broker : Uri)
                (topics : string seq)
                (groupId : string)
                (autoOffsetReset : Confluent.Kafka.AutoOffsetReset option)
                (fetchMaxBytes : int option)
                (fetchMinBytes : int option)
                (statisticsInterval : TimeSpan option)
                (autoCommitInterval : TimeSpan option)
                (custom : KeyValuePair<string, string> list option)
                (maxBatchSize : int option)
                (maxBatchDelay : TimeSpan option)
                (minInFlightBytes : int64 option)
                (maxInFlightBytes : int64 option) =
                KafkaConsumerConfig.Create(
                    clientId = clientId,
                    broker = broker,
                    topics = topics,
                    groupId = groupId,
                    ?autoOffsetReset = autoOffsetReset,
                    ?fetchMaxBytes = fetchMaxBytes,
                    ?fetchMinBytes = (fetchMinBytes |> Option.map Nullable.op_Implicit),
                    ?statisticsInterval = statisticsInterval,
                    ?autoCommitInterval = autoCommitInterval,
                    ?custom = custom,
                    ?maxBatchSize = maxBatchSize,
                    ?maxBatchDelay = maxBatchDelay,
                    ?minInFlightBytes = minInFlightBytes,
                    ?maxInFlightBytes = maxInFlightBytes)

            create 
            <!> clientId
            <*> broker
            <*> topics
            <*> groupId
            <*> autoOffsetReset
            <*> fetchMaxBytes
            <*> fetchMinBytes
            <*> statisticsInterval
            <*> offsetCommitInterval
            <*> custom
            <*> maxBatchSize
            <*> maxBatchDelay
            <*> minInFlightBytes
            <*> maxInFlightBytes

        parameters 
        |> esnureParameters prefix config
        |> Result.bind build