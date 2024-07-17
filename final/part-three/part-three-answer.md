* First run
  * `--driver-memory 2G --executor-memory 4G --executor-cores 1 --total-executor-cores 20`
  * Your Expectation: 4 hr
  * Your results/runtime:
    Explaination : This is basically command to limit resources.20 CPU cores available to executing tasks and 1 core per executor and 4g executor memory. We can able to see the total executor cores available for this application is low also 1 core per executor is also low so to execute and to give output it'll take time. In order to make execution faster and to run our application faster we need to increase executor cores.
 
* Second run
  * `--driver-memory 10G --executor-memory 12G --executor-cores 2`
  * Your Expectation: 1 hr
  * Your results/runtime:
    Explaination : We can able to see visible difference in driver memory, executor memory as well. This command allocate more driver memory its 10g, executor cores as 2 and executor memory as 12g. With this much memory application can process more data. Runtime for Application decreased. Also Executor cores plays important role in execution.faster than first case.

* Third run
  * `--driver-memory 4G --executor-memory 4G --executor-cores 2 --total-executor-cores 40`
  * Your Expectation: 45 min
  * Your results/runtime:
    Explaination : Visible difference over here is increased total executor cores and cores per executor also driver memory. Which will allow more parallelization and application will take less execution time. So execution time decrease and so it shows positive impact on execution time. Increase in drivers and cores will reduce application time and increase performance so basically it depends on that. execution time is faster than other two cases.