# GG_Sessions
A scalable solution for parsing JSON log data and deriving insights from it

The input is a single JSON file with multiple objects i.e {},{},{} ...
For better understanding output at each logical stage is written to disk.

The basic logic as follows:

    1.Read the data as sparkSQL Dataframe, extract relevant fields and flatten it.
    Save to (ggevent.log -> data)
    2.The dataset is split into multiple datasets.(By gameID)
    Save to (<gameID>.txt)
    3.Sorting within groups(grouped by deviceID) is done (sort by timestamp).
    4.Only first ggstart/ggstop is registered.
      Consecutive ggstop/start are discarded.
    5.Duration is calcuated(ggstop - ggstart) & also break period(next ggstart - ggstop)  <gameID>_sessions.txt
    6.Each Record is assigned a Session_Id which changes if break>30 or device_Id changes.
    7.Filter(duration>30)
      min(ggstart),max(ggstop),sum(duration) group by Session_Id
      Save to <gameID>.csv (in HDFS)
    8. avg & count and save to result.txt 

ToDO:

    1.Document and Refactor the code. 
    2.Use Maven/sbt to Build the Project
     Env: Java 8 | Scala 2.11 | Spark 2.0.2
