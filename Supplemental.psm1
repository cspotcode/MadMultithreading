# Set-StrictMode -Version Latest
# $ErrorActionPreference = 'Stop'

# TODO make the function declaration code match original madmultithreading

function CreateFunctionDefiner
    {
    param(
        $DeclaredFunctionName,
        $Function)
    
    if($Function -is [management.automation.commandinfo]) {
        $Command = $Function
    } else {
        write-verbose "getting function $Function"
        $Command = Get-Command $Function -ErrorAction SilentlyContinue
    }
    If ( $Command.ModuleName )
        {
        $FunctionDefinition = "Set-Alias -Name $DeclaredFunctionName -value " + $Command.Name
        }
    ElseIf ( $Command.CommandType -eq 'Function' )
        {
        $FunctionDefinition = "function $DeclaredFunctionName { " + $Command.Definition + ' }'
        }
    
    #  Else (Function is not defined in a module and is not a script-defined function)
    #    Throw error
    Else
        {
        Write-Error -Message "Unable to parse function [$Function]."
        }

    #  Convert wrapped function to scriptblock
    return [scriptblock]::Create( $FunctionDefinition )
    }
function New-MadThreadPool
    {
    [cmdletbinding()]
    Param (
        [int] $Threads = 2,
        $InitializeFunction,
        [hashtable] $InitializeParameters
    )

    Function noop {}
    if(-not $InitializeFunction)
        {
        $InitializeFunction = Get-Command noop
        }
    $DeclareInitialize = CreateFunctionDefiner Invoke-Initialize $InitializeFunction

    #  Define script to run in each thread
    $ThreadScript =
        {
        Param(
            [System.Collections.Concurrent.BlockingCollection[PSObject]]
            $InputQueue,

            [System.Collections.Concurrent.ConcurrentDictionary[Int,String]]
            $ThreadStatus,

            [Scriptblock]
            $DeclareInitialize,

            [Hashtable]
            $InitializeParameters
            )

        . $DeclareInitialize
        if($InitializeParameters.Keys) {
            Invoke-Initialize @InitializeParameters
        } else {
            Invoke-Initialize
        }

        #  Add self to list of running threads
        $ThreadID = [appdomain]::GetCurrentThreadId()
        $ThreadStatus[$ThreadID] = 'Waiting'

        #  For each Item in queue...
        #  (If queue is empty, will wait for item. If queue is closed, loop ends.)
        ForEach ( $Item in $InputQueue.GetConsumingEnumerable() )
            {
            $ThreadStatus[$ThreadID] = 'Processing'

            $Result = @{ Index = $Item.Index; Value = $Null; Error = $Null }

            # #  Convert wrapped function to scriptblock
            # $ScriptBlock = [scriptblock]::Create( $Item.ScriptBlockText )
            . $Item.DeclareFunction

            #  Call wrapped function, with or without additional parameters
            try
                {
                $Parameters = $Item.Parameters
                If ( $Parameters.Keys )
                    {
                    $Result.Value = $Item.Value | Invoke-WrappedFunction @Parameters
                    }
                Else
                    {
                    $Result.Value = $Item.Value | Invoke-WrappedFunction
                    }
                }
            catch
                {
                $Result.Error = $_
                }

            #  Return result
            if($Item.ResultQueue)
                {
                $Item.ResultQueue.Add( $Result )
                }

            $ThreadStatus[$ThreadID] = 'Waiting'
            }

        [void]$ThreadStatus.TryRemove( $ThreadId, [ref]$Null )
        
        }
    
    $InputQueue = [System.Collections.Concurrent.BlockingCollection[PSObject]]@{}
    $ThreadStatus = [System.Collections.Concurrent.ConcurrentDictionary[Int,String]]@{}

    $ThreadPool = [PsCustomObject]@{
        #  Collection for thread references        
        RunningThreads = @()
        #  Number of threads must be greater than 0
        Threads = [math]::Max( $Threads, 1 )
        #  Cross-thread objects
        InputQueue = $InputQueue
        ThreadStatus = $ThreadStatus
        #  Create runspace pool
        RunspacePool = [runspacefactory]::CreateRunspacePool( 1, $Threads )
        ThreadScript = $ThreadScript
        # Parameters passed to each thread's ThreadScript
        ThreadParameters = @{
            InputQueue = $InputQueue
            ThreadStatus = $ThreadStatus
            DeclareInitialize = $DeclareInitialize
            InitializeParameters = $InitializeParameters }
        }
    
    $ThreadPool.RunspacePool.Open()

    return $ThreadPool
    }


Function Invoke-InMadThreadPool {
    [cmdletbinding()]
    param(
        $ThreadPool,
        # [ScriptBlock]$ScriptBlock,
        $Function,
        [hashtable]$Parameters,
        [Switch]$NoWaitForResults,
        [Switch]$NoSort,
        [Parameter(ValueFromPipeline)]
        $InputObject
    )
    Begin
        {
        #  Index of next input object.  Also total number of sent Items.
        $Index = 0
        #  Total received results
        $ReceivedCount = 0
        #  Index of next sorted result
        $ResultIndex = 0
        #  Result object (needs to exist before using as a [ref] variable)
        $Result = [pscustomobject]@{}
        #  Collection for results
        $Results = @{}
        $ResultQueue = if($NoWaitForResults)
            {
            $null
            }
        else
            {
            [System.Collections.Concurrent.BlockingCollection[PSObject]]@{}
            }

        $DeclareFunction = CreateFunctionDefiner 'Invoke-WrappedFunction' $Function

        Function WriteResult($Result)
            {
            Write-Verbose "Writing result #$( $Result.Index ) to output stream"
            # Filter out nulls
            If ( $Result.Value -ne $Null )
                {
                $Result.Value
                }

            #  If an error was returned, write to error stream
            If ( $Result.Error )
                {
                Write-Error -ErrorRecord $Result.Error
                }
            }
        }

    Process
        {
        try
            {
            #  For each input object (looping to handle non-pipeline array input)
            ForEach ( $InputElement in $InputObject )
                {

                Write-Verbose ('Processing input element: ' + $InputElement)

                #  If we are not yet at max thread count and
                #  there are no threads waiting for work
                #    Start another thread
                If ( $ThreadPool.RunningThreads.Count -lt $ThreadPool.Threads -and
                        $ThreadPool.ThreadStatus.Values -notcontains 'Waiting' )
                    {
                    Write-Verbose 'Starting thread'
                    #  Create thread
                    $PowerShell = [PowerShell]::Create()
                    $PowerShell.RunspacePool = $ThreadPool.RunspacePool
                    [void]$PowerShell.AddScript( $ThreadPool.ThreadScript )
                    [void]$PowerShell.AddParameters( $ThreadPool.ThreadParameters )
                    #  Start thread
                    $Handler = $PowerShell.BeginInvoke()
                    #  Add thread hooks to collection
                    $ThreadPool.RunningThreads += [PSCustomObject]@{
                        PowerShell = $PowerShell
                        Handler    = $Handler }
                    }
                
                Write-Verbose 'Adding object to input queue'
                #  Add input object to input queue
                #  (Adding an index so results can be returned in the correct order)
                $ThreadPool.InputQueue.Add( @{
                    ResultQueue = $ResultQueue
                    DeclareFunction = $DeclareFunction
                    Parameters = $Parameters
                    Index = $Index++
                    Value = $InputElement } )

                if(-not $NoWaitForResults)
                    {
                    #  Check result queue
                    #    Put any results in the results collection
                    #    Repeat until empty
                    While ( $ResultQueue.TryTake( [ref]$Result ) )
                        {
                        $ReceivedCount++
                        Write-Verbose "Received result #$ReceivedCount (Index #$( $Result.Index )) from threadpool"
                        #  If not sorting, return result immediately
                        If ( $NoSort )
                            {
                            WriteResult $Result
                            }

                        #  else sorting: save result to collection
                        Else
                            {
                            $Results[$Result.Index] = $Result
                            }
                        }

                    #  If sorting, process results collection
                    If ( -not $NoSort )
                        {

                        #  Check results collection
                        #    If the result for the next result index is found
                        #      Process it
                        #    Repeat as needed
                        While ( $Results[$ResultIndex] )
                            {
                            WriteResult $Results[$ResultIndex]

                            #  Remove processed result from results collection
                            $Results.Remove( $ResultIndex )

                            #  Increment next result index to process
                            $ResultIndex++
                            }
                        }
                    }
                }
            }
        finally
            {
            #  Catch Ctrl-C
            If ( -not $? )
                {
                # TODO pretty sure no cleanup is required here because we are not responsible
                # for closing the threadpool at this time.
                }
            }
        }
    End
        {
        if(-not $NoWaitForResults)
            {
            #  For each result in result queue
            #  (Will wait for additional results until queue is closed)
            # Detect when all results received; close ResultQueue
            # Threads cannot do this themselves because they receive requests from multiple
            # invoke-inthreadpool functions
            if($ReceivedCount -ge $Index)
                {
                $ResultQueue.CompleteAdding()
                Write-Verbose 'Received all results; closing resultqueue'
                }

            ForEach ( $Result in $ResultQueue.GetConsumingEnumerable() )
                {
                $ReceivedCount++
                Write-Verbose "Received result #$ReceivedCount (Index #$( $Result.Index )) from threadpool"
                if($ReceivedCount -ge $Index)
                    {
                    Write-Verbose 'Received all results; closing resultqueue'
                    $ResultQueue.CompleteAdding()
                    }

                If ( $NoSort )
                    {
                    WriteResult $Results
                    }
                Else
                    {
                    #  Put result in results collection
                    $Results[$Result.Index] = $Result

                    #  Check results collection
                    #    If the result for the next result index is found
                    #      Process it
                    #    Repeat as needed
                    While ( $Results[$ResultIndex] )
                        {
                        WriteResult $Results[$ResultIndex]
                        $Results.Remove( $ResultIndex )
                        $ResultIndex++
                        }
                    }
                }
            }
        }

    } # Invoke-InThreadPool

Function Close-MadThreadPool {
    param(
        $ThreadPool
    )
    try
        {
        #  Close input queue
        $ThreadPool.InputQueue.CompleteAdding()
        }
    finally
        {
        #  Clean up
        $ThreadPool.RunningThreads | ForEach-Object { $_.PowerShell.Stop(); $_.PowerShell.Dispose() }
        $ThreadPool.RunspacePool.Dispose()
        }
} # Close-ThreadPool

<#

Each invoke-inworkerpool call should pass its own result queue to worker threads.
That way caller only gets results relevant to what it tried to execute.

Re-add FunctionDefinition to New-ThreadPool
It will be an "init script" for each thread.
#>