# pair_reporting
data, workflows for pair performance reporting

Scripts and results are in th ``N:\Depts\Share\UK Alpha Team\Analytics`` directory.

A task is setup on the Windows Task Manager to run every day at 6:30 in the morning:

![image](https://user-images.githubusercontent.com/1358190/41651469-8a890876-7478-11e8-9341-8c5563304c76.png)

The task is setup to run Rscript.exe:

``"C:\Program Files\R\R-3.4.3\bin\Rscript.exe"``

with this parameter:

``-e source('N:/Depts/Share/UK\x20Alpha\x20Team/Analytics/Rscripts/workflow.R')``

``workflow.R`` then executes individual workflow steps.

The  ``N:\Depts\Share\UK Alpha Team\Analytics\Rscripts`` directory workflow
scripts.


| File | Description |
|----------|----------|
| ``workflow.R`` | the start script  |
| ``workflow.log`` | results log |



